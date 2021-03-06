package escheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/stonever/escheduler/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/zap"
	"os"
	"path"
	"sync"
	"time"
)

const (
	ActionNew     = 1
	ActionDeleted = 2
)

var (
	ErrWorkerStatusNotInBarrier = errors.New("worker status is not runnable")
	ErrWorkerNumExceedMaximum   = errors.New("worker num exceed maximum")
)

type TaskChange struct {
	Action int // 1 new 2 deleted
	Task   Task
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
		return t.Task.Abbr, true
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
	Start(ctx context.Context) error
	Status() int
	WatchTask() <-chan WatchEvent
	WatchStatus(chan<- int)
	TryLeaveBarrier() error
	Stop()
}

type workerInstance struct {
	Node
	name string

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
	log.Info("BroadcastStatus", zap.Any("status", w.status))
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

func (w *workerInstance) Start(ctx context.Context) error {
	var (
		err error
	)
	var keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
	defer w.BroadcastStatus(WorkerStatusDead)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:

		}
		keepRespChan, err = w.register(ctx, w.workerPath, w.key())
		if err != nil {
			log.Error("failed to registerWorker", zap.String("worker name", w.name), zap.Error(err))
			time.Sleep(time.Second)
			continue
		}
		w.BroadcastStatus(WorkerStatusRegister)
		break
	}

	go func() {
		key := GetWorkerBarrierLeftKey(w.RootName)
		if resp, _ := w.client.KV.Get(ctx, key); len(resp.Kvs) > 0 {
			log.Info("no need to gotoBarrier", zap.String("worker", w.name), zap.String("barrier status", resp.Kvs[0].String()))
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
	var shouldBreak bool
	for {
		if shouldBreak {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case status, _ := <-c:
			if status == WorkerStatusNew {
				return errors.New("got empty status")
			}
			if status == WorkerStatusInBarrier {
				shouldBreak = true
				break
			}
		}

	}
	log.Info("worker enter double Barrier, begin to watch my task path", zap.String("worker", w.name), zap.Error(err))
	w.watch(ctx, keepRespChan)
	return nil
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
	log.Info("worker waiting double Barrier", zap.String("worker", w.name), zap.Int("num", num))
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
	return path.Join(w.workerPath, w.name)
}
func (w *workerInstance) Stop() {
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
func (w *workerInstance) Del(task Task) {
	w.taskChan <- TaskChange{Action: ActionDeleted, Task: task}
}
func (w *workerInstance) watch(ctx context.Context, keepRespChan <-chan *clientv3.LeaseKeepAliveResponse) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// ???watchChan???????????????task???????????????????????????????????????????????????
	resp, err := w.client.KV.Get(ctx, w.taskPath, clientv3.WithPrefix())
	if err != nil {
		err = errors.Wrapf(err, "get worker job list failed")
		return
	}
	for _, kvPair := range resp.Kvs {
		task, err := ParseTaskFromKV(kvPair.Key, kvPair.Value)
		if err != nil {
			err = errors.Wrapf(err, "Unmarshal task value:%s", kvPair.Key)
			continue
		}
		w.taskChan <- TaskChange{Task: task, Action: ActionNew}
	}
	watchStartRevision := resp.Header.Revision + 1
	watchChan := w.client.Watcher.Watch(ctx, w.taskPath, clientv3.WithPrefix(), clientv3.WithRev(watchStartRevision))
	for {
		select {
		case <-ctx.Done():
			return
		case keepResp := <-keepRespChan:
			// ???keepResp==nil?????????????????????????????????????????????ctx cancel??????etcd????????????
			if keepResp == nil {
				return
			}
		case watchResp := <-watchChan:
			for _, watchEvent := range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					// ??????????????????
					task, err := ParseTaskFromKV(watchEvent.Kv.Key, watchEvent.Kv.Value)
					if err != nil {
						log.Error("[WatchJob] Unmarshal task value:%s error:%s", zap.ByteString("key", watchEvent.Kv.Key), zap.Error(err))
						continue
					}
					w.Add(task)
				case mvccpb.DELETE:
					// ??????delete event
					task, err := ParseTaskFromKV(watchEvent.Kv.Key, watchEvent.Kv.Value)
					if err != nil {
						log.Error("[WatchJob] Unmarshal task value:%s error:%s", zap.ByteString("key", watchEvent.Kv.Key), zap.Error(err))
						continue
					}
					w.Del(task)
				default:
					log.Warn("[WatchJob] ????????????????????????:%s", zap.Any("event", watchEvent.Type))
				}
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
	var name string
	if len(node.CustomName) > 0 {
		name = node.CustomName
	} else {
		ip, err := GetLocalIP()
		if err != nil {
			return nil, errors.Wrapf(err, "cannot give the name to scheduler")
		}
		pid := os.Getpid()
		name = fmt.Sprintf("%s-%d", ip, pid)
	}

	worker := workerInstance{
		Node:           node,
		name:           name,
		workerPath:     path.Join("/", node.RootName, workerFolder) + "/",
		taskPath:       path.Join("/", node.RootName, taskFolder, name) + "/",
		status:         WorkerStatusNew,
		leavingBarrier: make(chan struct{}, 1),
	}
	// ????????????
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
func (w *workerInstance) register(ctx context.Context, workerPath, workerKey string) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	session, err := concurrency.NewSession(w.client)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to new session")
	}
	defer session.Close()
	key := getWorkerRegisterMutexKey(w.RootName)
	m := concurrency.NewMutex(session, key)
	if err := m.Lock(ctx); err != nil {
		return nil, errors.Wrapf(err, "failed to lock")
	}
	defer m.Unlock(context.TODO())

	resp, err := w.client.KV.Get(ctx, workerPath, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) >= w.MaxNum-1 {
		return nil, errors.Wrapf(ErrWorkerNumExceedMaximum, "now worker total is %d >= max is %d", len(resp.Kvs), w.MaxNum-1)
	}
	// ????????????
	leaseResp, err := w.client.Lease.Grant(ctx, w.TTL)
	if err != nil {
		err = errors.Wrapf(err, "create worker alive lease error")
		return nil, err
	}
	keepRespChan, err := w.client.Lease.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return nil, err
	}
	// ?????????etcd
	_, err = w.client.KV.Put(ctx, workerKey, "running", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return nil, err
	}
	return keepRespChan, nil
}

// getWorkerRegisterMutexKey example: /20220704/worker_register_mutex
func getWorkerRegisterMutexKey(rootName string) string {
	return path.Join("/", rootName, "worker_register_mutex")
}
