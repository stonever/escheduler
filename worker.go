package escheduler

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"time"

	"log/slog"

	"github.com/pkg/errors"
	"github.com/sourcegraph/conc"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/atomic"
)

const (
	ActionNew     = 1
	ActionDeleted = 2
)

var (
	ErrWorkerNumExceedMaximum = errors.New("worker num exceed maximum")
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

type Worker struct {
	Node

	// path
	workerPath string // eg. /20220624/worker/
	taskChan   chan WatchEvent
	taskPath   string // eg. 20220624/task/192.168.193.131-101576/
	status     atomic.String

	isAllRunning bool

	leavingBarrier chan struct{}

	// goroutine control
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func (w *Worker) Tasks(ctx context.Context) (map[string]struct{}, error) {
	prefix := w.taskPath
	ret := make(map[string]struct{})
	resp, err := w.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get etcd kv")
	}
	for _, value := range resp.Kvs {
		abbr, err := parseTaskIDFromTaskKey(w.RootName, string(value.Key))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse abbr:%s", value.Key)
		}
		ret[abbr] = struct{}{}

	}
	return ret, nil
}

func (w *Worker) WatchTask() <-chan WatchEvent {
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

func (w *Worker) Status() string {
	return w.status.Load()
}

func (w *Worker) keepOnline() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:

		}
		err := w.register()
		w.logger.ErrorContext(w.ctx, "failed to register worker", "error ", err)
		time.Sleep(time.Second)
	}
}

func (w *Worker) TryLeaveBarrier(d time.Duration) bool {
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	select {
	case w.leavingBarrier <- struct{}{}:
		return true
	case <-ticker.C:
		return false
	}
}
func (w *Worker) IsAllRunning() bool {
	return w.isAllRunning
}
func (w *Worker) SetStatus(status string) {

	old := w.status.Swap(status)
	w.logger.Info("worker status changed", "new", status, "old", old)
}
func (w *Worker) Start() {
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
			if w.Status() != WorkerStatusRegistered {
				continue
			}
			w.tryToBarrier()
		}
	})
	for status := ""; status != WorkerStatusInBarrier && status != WorkerStatusLeftBarrier; status = w.Status() {
		time.Sleep(time.Second)
	}
	w.logger.Info("all workers have been in double Barrier, begin to watch my own task path")
	wg.Go(func() {
		err := w.watch()
		if err != nil {
			w.logger.Error("worker watch error", err)
		}
	})
	return
}
func (w *Worker) tryToBarrier() {
	leftBarrierKey := GetWorkerBarrierLeftKey(w.RootName)
	ctx := w.ctx
	if resp, _ := w.client.KV.Get(ctx, leftBarrierKey); len(resp.Kvs) > 0 {
		w.logger.Info("no need to enter barrier", w.Name, resp.Kvs[0].String())
		w.SetStatus(WorkerStatusLeftBarrier)
		return
	}
	err := w.gotoBarrier(ctx)
	if err != nil {
		w.logger.Error("failed to gotoBarrier", "error", err)
	}
}
func (w *Worker) gotoBarrier(ctx context.Context) error {
	num := w.MaxNumNodes
	barrier := GetWorkerBarrierName(w.RootName)
	w.logger.Info("enter the barrier...", "worker", w.Name, "barrier_key", barrier)
	s, err := concurrency.NewSession(w.client)
	if err != nil {
		return err
	}
	defer s.Close()

	b := recipe.NewDoubleBarrier(s, barrier, num)
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
func (w *Worker) key() string {
	return path.Join(w.workerPath, w.Name)
}
func (w *Worker) Stop() {
	w.SetStatus(WorkerStatusDead)
	w.cancel()
	_ = w.client.Close()
}
func (w *Worker) Add(task Task) {
	w.taskChan <- TaskChange{Action: ActionNew, Task: task}
}
func (w *Worker) Del(id string) {
	var tc = TaskChange{Action: ActionDeleted}
	tc.ID = id
	w.taskChan <- tc
}
func (w *Worker) watch() error {
	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()

	watcher, err := NewWatcher(ctx, w.client, w.taskPath)
	if err != nil {
		return err
	}
	// add existed task
	for _, kvPair := range watcher.IncipientKVs {
		// id = kvPair.Key
		task, err := parseTaskFromValue(kvPair.Value)
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
			w.logger.Info("check watcher live", "is_blocking", watcher.blocking)
		case event, ok := <-watcher.EventChan:
			if !ok {
				return errors.Errorf("watcher stopped")
			}
			switch event.Type {
			case mvccpb.PUT:
				// 任务新建事件
				// id = kvPair.Key
				task, err := parseTaskFromValue(event.Kv.Value)
				if err != nil {
					w.logger.Error("[watch] failed to parse created task", "key", event.Kv.Key, "value", event.Kv.Value, "error", err)
					continue
				}
				w.Add(task)
			case mvccpb.DELETE:
				// 任务delete event
				// id = kvPair.Key
				taskID, err := parseTaskIDFromTaskKey(w.RootName, string(event.Kv.Key))
				if err != nil {
					w.logger.Error("[watch] failed to parse deleted task", "key", event.Kv.Key, "value", event.Kv.Value, "error", err)
					continue
				}
				w.Del(taskID)
			default:
				w.logger.Warn("[watch] unsupported event case:%s", "event", event.Type)
			}

		}
	}
}

// NewWorker create a worker
func NewWorker(node Node) (*Worker, error) {
	err := node.Validate()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot give the name to scheduler")
	}

	worker := Worker{
		Node:           node,
		workerPath:     path.Join("/", node.RootName, workerFolder) + "/",
		taskPath:       path.Join("/", node.RootName, taskFolder, node.Name) + "/",
		leavingBarrier: make(chan struct{}, 1),
		logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{AddSource: true})).
			With("logger", "esched").With("worker", node.Name),
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
// condition of the func returned is 1. worker key has been deleted
// 2. keepRespChan is closed
func (w *Worker) register() error {
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
	if len(resp.Kvs) >= w.MaxNumNodes-1 {
		return errors.Wrapf(ErrWorkerNumExceedMaximum, "now worker total is %d >= max is %d", len(resp.Kvs), w.MaxNumNodes-1)
	}
	// 创建租约
	leaseResp, err := w.client.Grant(ctx, w.TTL)
	if err != nil {
		err = errors.Wrapf(err, "create worker alive lease error")
		return err
	}
	// 注册到etcd
	putResp, err := w.client.KV.Put(ctx, workerKey, WorkerValueRunning, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return err
	}
	keepAlive, err := w.client.Lease.KeepAlive(ctx, leaseResp.ID)
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
	w.logger.Info("succeeded to register worker", workerKey, w.TTL)
	w.SetStatus(WorkerStatusRegistered)
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.logger.Info("worker is alive", "worker key", workerKey)
		case _, ok := <-keepAlive:
			// keep the lease alive until client error or cancelled context
			// closed keepRespChan  can be a fatal error,other error should only be logged
			if !ok {
				return errors.Errorf("keepRespChan is closed,the worker has been lost")
			}
		case resp, ok := <-watchChan:
			if !ok {
				w.logger.Warn("watchChan is closed,the worker may be lost", resp.Err())
				// If the requested revision is 0 or unspecified, the returned channel will
				// return watch events that happen after the server receives the watch request.
				watchChan = w.client.Watch(ctx, workerKey)
				continue
			}
			if resp.Canceled || resp.Err() != nil {
				// for example,if etcd node restart, resp will raise error, mvcc: requested revision has been compacted
				w.logger.Error("watchChan resp error", "Canceled", resp.Canceled, "error", resp.Err(), "resp", resp)
				watchChan = w.client.Watch(ctx, workerKey)
				continue
			}
			for _, watchEvent := range resp.Events {
				if watchEvent.IsCreate() {
					w.logger.Info("worker watch event: create")
					continue
				}
				if watchEvent.Type == mvccpb.DELETE {
					w.logger.Info("worker watch event: delete")
					return errors.Errorf("worker key has been delete,the worker has been deleted")
				}
			}
		}
	}

}

// getWorkerRegisterMutexKey example: /20220704/worker_register_mutex
func getWorkerRegisterMutexKey(rootName string) string {
	return path.Join("/", rootName, "worker_register_mutex")
}
