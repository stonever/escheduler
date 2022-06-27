package escheduler

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	Task   RawData
}

// Worker instances are used to handle individual task.
// It also provides hooks for your worker session life-cycle and allow you to
// trigger logic before or after the worker loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.
type Worker interface {
	// Start is run at the beginning of a new session, before ConsumeClaim.
	Start(ctx context.Context) (chan TaskChange, error)
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

func (w workerInstance) Start(ctx context.Context) (chan TaskChange, error) {
	var (
		leaseResp    *clientv3.LeaseGrantResponse
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		err          error
	)
	// 创建租约
	leaseResp, err = w.client.Lease.Grant(ctx, w.TTL)
	if err != nil {
		log.Errorf("create worker alive lease error:%s", err)
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

	go w.watch(ctx, keepRespChan)
	w.taskChan = make(chan TaskChange)
	return w.taskChan, nil
}
func (w workerInstance) key() string {
	return path.Join(w.workerPath, w.name)
}
func (w *workerInstance) Stop() {
	close(w.closed)
}
func (w *workerInstance) Add(task RawData) {
	w.taskChan <- TaskChange{Action: ActionNew, Task: task}
}
func (w *workerInstance) Del(task RawData) {
	w.taskChan <- TaskChange{Action: ActionDeleted, Task: task}
}
func (w *workerInstance) watch(ctx context.Context, keepRespChan <-chan *clientv3.LeaseKeepAliveResponse) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// 在watchChan产生之前，task发生了增删，也会被感知到，进行同步
	resp, err := w.client.KV.Get(ctx, w.taskPath, clientv3.WithPrefix())
	if err != nil {
		log.Errorf("[watch] get worker job list failed. %s", err)
		return
	}
	for _, kvPair := range resp.Kvs {
		task, err := ParseTaskFromTaskKey(string(kvPair.Key))
		if err != nil {
			log.Errorf("[WatchJob] Unmarshal task value:%s error:%s", kvPair.Key, err)
			continue
		}
		w.taskChan <- TaskChange{Task: task, Action: 1}
	}
	watchStartRevision := resp.Header.Revision + 1
	watchChan := w.client.Watcher.Watch(ctx, w.taskPath, clientv3.WithPrefix(), clientv3.WithRev(watchStartRevision))
	for {
		select {
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
					task, err := ParseTaskFromTaskKey(string(watchEvent.Kv.Key))
					if err != nil {
						log.Errorf("[WatchJob] Unmarshal task value:%s error:%s", watchEvent.Kv.Key, err)
						continue
					}
					w.Add(task)
				case mvccpb.DELETE:
					// 任务新建事件
					task, err := ParseTaskFromTaskKey(string(watchEvent.Kv.Key))
					if err != nil {
						log.Errorf("[WatchJob] Unmarshal task value:%s error:%s", watchEvent.Kv.Key, err)
						continue
					}
					w.Del(task)
				default:
					log.Warnf("[WatchJob] 不支持的事件类型:%s", watchEvent.Type)
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
