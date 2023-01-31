package escheduler

import (
	"context"
	"encoding/json"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/stonever/escheduler/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/zap"
)

const (
	ActionNew     = 1
	ActionDeleted = 2
)

var (
	ErrWorkerStatusNotInBarrier = errors.New("worker status is not in barrier")
	ErrWorkerNumExceedMaximum   = errors.New("worker num exceed maximum")
)

type TaskChange struct {
	Action int // 1 new 2 deleted
	Task
}

func (t TaskChange) String() string {
	str, _ := json.Marshal(t)
	return string(str)
}

type WatchEvent interface {
	CreatedTask() (Task, bool)
	DeletedTask() (string, bool)
}

func (t TaskChange) CreatedTask() (Task, bool) {
	if t.Action == ActionNew {
		return t.Task, true
	}
	return Task{}, false
}
func (t TaskChange) DeletedTask() (string, bool) {
	if t.Action == ActionDeleted {
		return t.Task.ID, true
	}
	return "", false
}

// Worker instances are used to handle individual task.
// It also provides hooks for your worker session life-cycle and allow you to
// trigger logic before or after the worker loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.
type Worker interface {
	// Start is run at the beginning of a new session, before ConsumeClaim.
	Start()
	Status() int
	WatchStatus(chan<- int)
	Tasks(ctx context.Context) (map[string]struct{}, error)
	WatchTask() <-chan WatchEvent
	TryLeaveBarrier() error
	Stop()
}

type workerInstance struct {
	Node

	// path
	workerPath string // eg. /20220624/worker/
	taskChan   chan WatchEvent
	taskPath   string // eg. 20220624/task/192.168.193.131-101576/
	status     int
	// watcher subscribe status change of worker
	watcher []chan<- int
	sync.RWMutex
	isAllRunning bool

	leavingBarrier chan struct{}

	// goroutine control
	ctx    context.Context
	cancel context.CancelFunc
}

func (w *workerInstance) Tasks(ctx context.Context) (map[string]struct{}, error) {
	prefix := w.taskPath
	ret := make(map[string]struct{})
	resp, err := w.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get etcd kv")
	}
	for _, value := range resp.Kvs {
		abbr, err := ParseTaskAbbrFromTaskKey(string(value.Key))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse abbr:%s", value.Key)
		}
		ret[abbr] = struct{}{}

	}
	return ret, nil
}

func (w *workerInstance) WatchTask() <-chan WatchEvent {
	return w.taskChan
}

// GetWorkerBarrierName /kline-pump/20220628/worker_barrier
func GetWorkerBarrierName(rootName string) string {
	return path.Join("/", rootName, workerBarrier)
}

// GetWorkerBarrierLeftKey /kline-pump-20220628/worker_barrier_left
func GetWorkerBarrierLeftKey(rootName string) string {
	return path.Join("/", rootName, "worker_barrier_left")
}

// GetSchedulerBarrierName /kline-pump/20220628/scheduler_barrier
func GetSchedulerBarrierName(rootName string) string {
	return path.Join("/", rootName, schedulerBarrier)
}

type WorkerConfig struct {
}

func (w *workerInstance) Status() int {
	return w.status
}
func (w *workerInstance) WatchStatus(c chan<- int) {
	w.Lock()
	defer w.Unlock()
	w.watcher = append(w.watcher, c)
	go sendStatus(c, w.status, time.Minute)
}
func (w *workerInstance) BroadcastStatus(status int) {
	w.RLock()
	defer w.RUnlock()
	log.Info("BroadcastStatus", zap.Any("status", w.status), zap.Int("watcher_count", len(w.watcher)))
	w.status = status
	for _, watcher := range w.watcher {
		go sendStatus(watcher, w.status, time.Minute)
	}
}
func sendStatus(c chan<- int, status int, timeout time.Duration) {
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	select {
	case c <- status:
		return
	case <-ticker.C:
		log.Error("send status to watcher timeout", zap.Duration("timeout", timeout))
		return
	}
}
func (w *workerInstance) RemoveWatcher(c chan int) {
	w.Lock()
	defer w.Unlock()
	i := 0
	for _, value := range w.watcher {
		if value != c {
			w.watcher[i] = value
			i++
		}
	}
	w.watcher = w.watcher[:i]
}

func (w *workerInstance) TryLeaveBarrier() error {
	if w.status == WorkerStatusInBarrier {
		select {
		case w.leavingBarrier <- struct{}{}:
		default:
		}

		return nil
	}
	return ErrWorkerStatusNotInBarrier
}
func (w *workerInstance) IsAllRunning() bool {
	return w.isAllRunning
}

func (w *workerInstance) Start() {
	var (
		err error
		ctx = w.ctx
	)
	defer w.BroadcastStatus(WorkerStatusDead)
	for {
		select {
		case <-ctx.Done():
			return
		default:

		}
		err = w.register(ctx, w.workerPath, w.key())
		if err != nil {
			log.Error("failed to register worker", zap.String("worker name", w.Name), zap.Error(err))
			time.Sleep(time.Second)
			continue
		}
		log.Info("register worker done", zap.String("worker name", w.Name))

		w.BroadcastStatus(WorkerStatusRegister)
		break
	}

	go func() {
		key := GetWorkerBarrierLeftKey(w.RootName)
		if resp, _ := w.client.KV.Get(ctx, key); len(resp.Kvs) > 0 {
			log.Info("no need to gotoBarrier", zap.String("worker", w.Name), zap.String("barrier status", resp.Kvs[0].String()))
			w.BroadcastStatus(WorkerStatusInBarrier)
			w.BroadcastStatus(WorkerStatusLeftBarrier)
			return
		}
		err := w.gotoBarrier(ctx)
		if err != nil {
			log.Error("failed to gotoBarrier", zap.Error(err))
		}
	}()
	c := make(chan int, 1)
	w.WatchStatus(c)
	for status := range c {
		if status == WorkerStatusInBarrier {
			w.RemoveWatcher(c)
			break
		}
	}
	log.Info("all workers have entered double Barrier, begin to watch my own task path", zap.String("worker", w.Name), zap.Error(err))
	err = w.watch(ctx)
	if err != nil {
		log.Error("worker watch error", zap.Error(err))
	}
	return
}
func (w *workerInstance) gotoBarrier(ctx context.Context) error {
	num := w.MaxNum
	barrier := GetWorkerBarrierName(w.RootName)
	s, err := concurrency.NewSession(w.client)
	if err != nil {
		return err
	}
	defer s.Close()

	b := recipe.NewDoubleBarrier(s, barrier, num)
	log.Info("worker waiting double Barrier", zap.String("worker", w.Name), zap.Int("num", num))
	err = b.Enter()
	if err != nil {
		return err
	}
	w.BroadcastStatus(WorkerStatusInBarrier)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.leavingBarrier:

	}
	// waiting signal from task manager
	err = b.Leave()
	if err != nil {
		return err
	}
	w.BroadcastStatus(WorkerStatusLeftBarrier)
	return nil
}
func (w *workerInstance) key() string {
	return path.Join(w.workerPath, w.Name)
}
func (w *workerInstance) Stop() {
	w.cancel()
	w.Lock()
	defer w.Unlock()
	if w.Status() == WorkerStatusDead {
		return
	}
	_ = w.client.Lease.Close()
	_ = w.client.Close()
	go w.BroadcastStatus(WorkerStatusDead)
}
func (w *workerInstance) Add(task Task) {
	w.taskChan <- TaskChange{Action: ActionNew, Task: task}
}
func (w *workerInstance) Del(id string) {
	var tc = TaskChange{Action: ActionDeleted}
	tc.ID = id
	w.taskChan <- tc
}
func (w *workerInstance) watch(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	watcher, err := NewWatcher(ctx, w.client, w.taskPath)
	if err != nil {
		return err
	}
	// ad existed task
	for _, kvPair := range watcher.IncipientKVs {
		// id = kvPair.Key
		task, err := ParseTaskFromValue(kvPair.Value)
		if err != nil {
			err = errors.Wrapf(err, "Unmarshal task key:%s", kvPair.Key)
			return err
		}
		w.Add(task)
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			log.Info("check watcher live", zap.Bool("blocking", watcher.Blocking))
		case event, ok := <-watcher.EventChan:
			if !ok {
				return errors.Errorf("watcher stopped")
			}
			switch event.Type {
			case mvccpb.PUT:
				// 任务新建事件
				// id = kvPair.Key
				task, err := ParseTaskFromValue(event.Kv.Value)
				if err != nil {
					log.Error("[watch] failed to parse created task", zap.ByteString("key", event.Kv.Key), zap.ByteString("value", event.Kv.Value), zap.Error(err))
					continue
				}
				w.Add(task)
			case mvccpb.DELETE:
				// 任务delete event
				// id = kvPair.Key
				taskID, err := ParseTaskAbbrFromTaskKey(string(event.Kv.Key))
				if err != nil {
					log.Error("[watch] failed to parse deleted task", zap.ByteString("key", event.Kv.Key), zap.ByteString("value", event.Kv.Value), zap.Error(err))
					continue
				}
				w.Del(taskID)
			default:
				log.Warn("[watch] unsupported event case:%s", zap.Any("event", event.Type))
			}

		}
	}

}

// NewWorker create a worker
func NewWorker(node Node) (Worker, error) {
	err := node.Validation()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot give the name to scheduler")
	}

	worker := workerInstance{
		Node:           node,
		workerPath:     path.Join("/", node.RootName, workerFolder) + "/",
		taskPath:       path.Join("/", node.RootName, taskFolder, node.Name) + "/",
		status:         WorkerStatusNew,
		leavingBarrier: make(chan struct{}, 1),
	}
	worker.ctx, worker.cancel = context.WithCancel(context.Background())
	// 建立连接
	worker.client, err = clientv3.New(node.EtcdConfig)
	if err != nil {
		return nil, err
	}
	worker.taskChan = make(chan WatchEvent)
	return &worker, nil
}

// register let multi workers are registered serializable
// if lock failed, will block rather than return err
// if reach maximum, return err and retry
func (w *workerInstance) register(ctx context.Context, workerPath, workerKey string) error {
	session, err := concurrency.NewSession(w.client)
	if err != nil {
		return errors.Wrapf(err, "failed to new session")
	}
	defer session.Close()
	key := getWorkerRegisterMutexKey(w.RootName)
	m := concurrency.NewMutex(session, key)
	err = m.Lock(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to lock")
	}
	defer func() {
		_ = m.Unlock(context.TODO())
	}()

	resp, err := w.client.KV.Get(ctx, workerPath, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return err
	}
	if len(resp.Kvs) >= w.MaxNum-1 {
		return errors.Wrapf(ErrWorkerNumExceedMaximum, "now worker total is %d >= max is %d", len(resp.Kvs), w.MaxNum-1)
	}
	// 创建租约
	leaseResp, err := w.client.Lease.Grant(ctx, w.TTL)
	if err != nil {
		err = errors.Wrapf(err, "create worker alive lease error")
		return err
	}
	// 注册到etcd
	_, err = w.client.KV.Put(ctx, workerKey, WorkerValueRunning, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return err
	}
	keepRespChan, err := w.client.Lease.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return err
	}
	go func() {
		for _ = range keepRespChan {
		}
		log.Info("keepRespChan is closed")
	}()

	return nil
}

// getWorkerRegisterMutexKey example: /20220704/worker_register_mutex
func getWorkerRegisterMutexKey(rootName string) string {
	return path.Join("/", rootName, "worker_register_mutex")
}
