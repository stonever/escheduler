package escheduler

import (
	"context"
	"encoding/json"
	"path"
	"sync/atomic"
	"time"

	"github.com/sourcegraph/conc"

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
	Start() // Start is run at the beginning of a new session, before ConsumeClaim.
	Stop()
	Status() int32
	Tasks(ctx context.Context) (map[string]struct{}, error)
	WatchTask() <-chan WatchEvent
	TryLeaveBarrier() error
}

type workerInstance struct {
	Node

	// path
	workerPath string // eg. /20220624/worker/
	taskChan   chan WatchEvent
	taskPath   string // eg. 20220624/task/192.168.193.131-101576/
	status     atomic.Int32

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

func (w *workerInstance) Status() int32 {
	return w.status.Load()
}

func (w *workerInstance) keepOnline() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:

		}
		err := w.register()
		log.Error("failed to register worker", zap.String("worker name", w.Name), zap.Error(err))
		time.Sleep(time.Second)
	}
}

func (w *workerInstance) TryLeaveBarrier() error {
	if w.Status() == WorkerStatusInBarrier {
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
func (w *workerInstance) SetStatus(status int32) {

	old := w.status.Swap(status)
	log.Info("SetStatus", zap.Any("new", status), zap.Any("old", old))
}
func (w *workerInstance) Start() {
	var (
		wg conc.WaitGroup
	)
	defer func() {
		wg.Wait()
		w.SetStatus(WorkerStatusDead)
	}()

	wg.Go(func() {
		w.keepOnline()
	})
	wg.Go(func() {
		for {
			select {
			case <-w.ctx.Done():
				return
			default:

			}
			time.Sleep(time.Second)
			if w.Status() != WorkerStatusRegister {
				continue
			}
			log.Info("try to barrier", zap.String("worker", w.Name), zap.Int32("status", w.Status()))
			w.tryToBarrier()
		}
	})
	var status int32
	for status = 0; status != WorkerStatusInBarrier && status != WorkerStatusLeftBarrier; status = w.Status() {
		time.Sleep(time.Second)
	}
	log.Info("all workers have been in double Barrier, begin to watch my own task path", zap.String("worker", w.Name))
	wg.Go(func() {
		err := w.watch()
		if err != nil {
			log.Error("worker watch error", zap.Error(err))
		}
	})
	return
}
func (w *workerInstance) tryToBarrier() {
	key := GetWorkerBarrierLeftKey(w.RootName)
	ctx := w.ctx
	if resp, _ := w.client.KV.Get(ctx, key); len(resp.Kvs) > 0 {
		log.Info("no need to gotoBarrier", zap.String("worker", w.Name), zap.String("barrier status", resp.Kvs[0].String()))
		w.SetStatus(WorkerStatusLeftBarrier)
		return
	}
	err := w.gotoBarrier(ctx)
	if err != nil {
		log.Error("failed to gotoBarrier", zap.Error(err))
	}
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
	w.SetStatus(WorkerStatusInBarrier)
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
	w.SetStatus(WorkerStatusLeftBarrier)
	return nil
}
func (w *workerInstance) key() string {
	return path.Join(w.workerPath, w.Name)
}
func (w *workerInstance) Stop() {
	w.SetStatus(WorkerStatusDead)
	_ = w.client.Close()
}
func (w *workerInstance) Add(task Task) {
	w.taskChan <- TaskChange{Action: ActionNew, Task: task}
}
func (w *workerInstance) Del(id string) {
	var tc = TaskChange{Action: ActionDeleted}
	tc.ID = id
	w.taskChan <- tc
}
func (w *workerInstance) watch() error {
	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()

	watcher, err := NewWatcher(ctx, w.client, w.taskPath)
	if err != nil {
		return err
	}
	// add existed task
	for _, kvPair := range watcher.IncipientKVs {
		// id = kvPair.Key
		task, err := ParseTaskFromValue(kvPair.Value)
		if err != nil {
			err = errors.Wrapf(err, "Unmarshal task key:%s", kvPair.Key)
			return err
		}
		w.Add(task)
	}

	ticker := time.NewTicker(time.Minute * 10)
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
		leavingBarrier: make(chan struct{}, 1),
	}
	worker.SetStatus(WorkerStatusNew)
	worker.ctx, worker.cancel = context.WithCancel(context.Background())
	// pass worker's ctx to client
	node.EtcdConfig.Context = worker.ctx
	worker.client, err = clientv3.New(node.EtcdConfig)
	if err != nil {
		return nil, err
	}
	worker.taskChan = make(chan WatchEvent)
	return &worker, nil
}

// register let multi workers are registered serializable
// if lock failed, will block rather than return err
// if reach maximum, return err
func (w *workerInstance) register() error {
	var (
		ctx        = w.ctx
		workerPath = w.workerPath
		workerKey  = w.key()
		mutexKey   = getWorkerRegisterMutexKey(w.RootName)
	)
	session, err := concurrency.NewSession(w.client)
	if err != nil {
		return errors.Wrapf(err, "failed to new session")
	}
	defer session.Close()
	// locked, let multi workers are registered serializable
	m := concurrency.NewMutex(session, mutexKey)
	err = m.Lock(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to lock")
	}
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
	putResp, err := w.client.KV.Put(ctx, workerKey, WorkerValueRunning, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return err
	}
	keepRespChan, err := w.client.Lease.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return err
	}
	// it will be automatically unlocked after the session closes
	// if locker not obtained, error is etcdserver: key is not provided
	err = m.Unlock(ctx)
	if err != nil {
		return err
	}
	watchStartRevision := putResp.Header.Revision + 1
	watchChan := w.client.Watch(ctx, workerKey, clientv3.WithRev(watchStartRevision))
	log.Info("succeeded to register worker", zap.String("worker key", workerKey), zap.Int64("ttl", w.TTL))
	w.SetStatus(WorkerStatusRegister)
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Info("worker is alive", zap.String("worker key", workerKey))
		case _, ok := <-keepRespChan:
			if !ok {
				return errors.New("keepRespChan is closed,the worker has been lost")
			}
		case resp, ok := <-watchChan:
			if !ok {
				return errors.New("watchChan is closed, the worker has been lost")
			}
			if resp.Canceled {
				return errors.Errorf("watchChan is canceled with err:%s,the worker has been lost", resp.Err())
			}
			for _, watchEvent := range resp.Events {
				if watchEvent.IsCreate() {
					log.Info("worker watch event: create")
					continue
				}
				if watchEvent.Type == mvccpb.DELETE {
					log.Info("worker watch event: delete")
					return errors.Errorf("worker key has been delete,the worker has been lost")
				}
			}
		}
	}

}

// getWorkerRegisterMutexKey example: /20220704/worker_register_mutex
func getWorkerRegisterMutexKey(rootName string) string {
	return path.Join("/", rootName, "worker_register_mutex")
}
