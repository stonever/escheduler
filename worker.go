package escheduler

import (
	"context"
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
	"time"
)

const (
	ActionNew     = 1
	ActionDeleted = 2
)

type TaskChange struct {
	Action int // 1 new 2 deleted
	Task   Task
}

// Worker instances are used to handle individual task.
// It also provides hooks for your worker session life-cycle and allow you to
// trigger logic before or after the worker loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.
type Worker interface {
	// Start is run at the beginning of a new session, before ConsumeClaim.
	Start(ctx context.Context, cfg WorkerConfig) (chan TaskChange, error)
	Stop()
}

type workerInstance struct {
	Node
	name   string
	closed chan struct{} //

	// path
	workerPath string // eg. /20220624/worker
	taskChan   chan TaskChange
	taskPath   string // eg. 20220624/task/192.168.193.131-101576
}

// GetWorkerBarrierName /kline-pump/20220628/worker_barrier
func GetWorkerBarrierName(rootName string) string {
	return path.Join("/", rootName, workerBarrier)
}

// GetSchedulerBarrierName /kline-pump/20220628/scheduler_barrier
func GetSchedulerBarrierName(rootName string) string {
	return path.Join("/", rootName, schedulerBarrier)
}

type WorkerConfig struct {
}

func (w workerInstance) Start(ctx context.Context, cfg WorkerConfig) (chan TaskChange, error) {
	var (
		leaseResp    *clientv3.LeaseGrantResponse
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		err          error
	)
	// 创建租约
	leaseResp, err = w.client.Lease.Grant(ctx, w.TTL)
	if err != nil {
		err = errors.Wrapf(err, "create worker alive lease error")
		time.Sleep(time.Second)
		return nil, err
	}
	keepRespChan, err = w.client.Lease.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		time.Sleep(time.Second)
		return nil, err
	}

	// 注册到etcd
	key := w.key() // eg /20220624/worker/192.168.193.131-102082
	_, err = w.client.KV.Put(ctx, key, "running", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return nil, err
	}
	//for {
	//	//time.Sleep(time.Millisecond * 100)
	//	barrierName := GetSchedulerBarrierName(w.RootName)
	//	resp, err := w.client.KV.Get(ctx, barrierName)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	if len(resp.Kvs) == 0 {
	//		continue
	//	}
	//	schedulerBarrier := recipe.NewBarrier(w.client, barrierName)
	//	err = schedulerBarrier.Wait()
	//	if err != nil {
	//		return nil, err
	//	}
	//	break
	//}
	//
	//log.Info("worker enter schedulerBarrier", zap.String("worker", w.name), zap.String("barrier", barrierName))
	key = GetWorkerBarrierName(w.RootName)
	s, err := concurrency.NewSession(w.client)
	if err != nil {
		return nil, err
	}
	b := recipe.NewDoubleBarrier(s, key, w.MaxNum)
	log.Info("worker waiting double Barrier", zap.String("worker", w.name), zap.Int("num", w.MaxNum))
	err = b.Enter()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	log.Info("worker enter double Barrier", zap.String("worker", w.name), zap.Error(err))
	go w.watch(ctx, keepRespChan)
	w.taskChan = make(chan TaskChange)
	return w.taskChan, nil
}
func (w workerInstance) key() string {
	return path.Join(w.workerPath, w.name)
}
func (w *workerInstance) Stop() {
	_ = w.client.Lease.Close()
	_ = w.client.Close()
	close(w.closed)
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
	// 在watchChan产生之前，task发生了增删，也会被感知到，进行同步
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
		case <-w.closed:
			return
		case keepResp := <-keepRespChan:
			// 当keepResp==nil说明租约失效，产生的原因可能是ctx cancel或者etcd服务异常
			if keepResp == nil {
				w.Stop()
				return
			}
		case watchResp := <-watchChan:
			for _, watchEvent := range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT:
					// 任务新建事件
					task, err := ParseTaskFromKV(watchEvent.Kv.Key, watchEvent.Kv.Value)
					if err != nil {
						log.Error("[WatchJob] Unmarshal task value:%s error:%s", zap.ByteString("key", watchEvent.Kv.Key), zap.Error(err))
						continue
					}
					w.Add(task)
				case mvccpb.DELETE:
					// 任务delete event
					task, err := ParseTaskFromKV(watchEvent.Kv.Key, watchEvent.Kv.Value)
					if err != nil {
						log.Error("[WatchJob] Unmarshal task value:%s error:%s", zap.ByteString("key", watchEvent.Kv.Key), zap.Error(err))
						continue
					}
					w.Del(task)
				default:
					log.Warn("[WatchJob] 不支持的事件类型:%s", zap.Any("event", watchEvent.Type))
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
	// todo
	ip, err := GetLocalIP()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot give the name to scheduler")
	}
	pid := os.Getpid()
	name := fmt.Sprintf("%s-%d", ip, pid)
	worker := workerInstance{
		Node:       node,
		name:       name,
		closed:     make(chan struct{}),
		workerPath: path.Join("/", node.RootName, workerFolder),
		taskPath:   path.Join("/", node.RootName, taskFolder, name),
	}
	// 建立连接
	worker.client, err = clientv3.New(node.EtcdConfig)
	if err != nil {
		return nil, err
	}
	return &worker, nil
}
