package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"path"
	"strings"
	"time"
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
	Stop() error
}
type WorkerConfig struct {
	EtcdConfig clientv3.Config
	RootPath   string
	// TTL configures the session's TTL in seconds.
	// If TTL is <= 0, the default 60 seconds TTL will be used.
	NodeTTL int64
}

func (wc WorkerConfig) Validation() error {
	if len(wc.RootPath) == 0 {
		return errors.New("RootName is required")
	}
	return nil
}

type workerInstance struct {
	config    WorkerConfig
	client    *clientv3.Client
	name      string
	closeChan chan struct{} //

	// path
	workerPath     string
	taskChan       chan TaskChange
	taskPathPrefix string
}

func (w workerInstance) Start(ctx context.Context) (chan TaskChange, error) {
	var (
		leaseResp    *clientv3.LeaseGrantResponse
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		err          error
	)
	// 创建租约
	leaseResp, err = w.client.Lease.Grant(ctx, w.config.NodeTTL)
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
	key := w.key()

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
func (w workerInstance) Stop() error {
	//TODO implement me
	panic("implement me")
}
func (w workerInstance) Add(task RawData) {
	w.taskChan <- TaskChange{Action: 1, Task: task}
}
func (w workerInstance) Del(task RawData) {
	w.taskChan <- TaskChange{Action: 2, Task: task}
}
func (w workerInstance) watch(ctx context.Context, keepRespChan <-chan *clientv3.LeaseKeepAliveResponse) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := w.client.KV.Get(ctx, w.taskPathPrefix, clientv3.WithPrefix())
	if err != nil {
		log.Errorf("[watch] get worker job list failed. %s", err)
		return
	}
	for _, kvPair := range resp.Kvs {
		task, err := ParseTaskFromKey(string(kvPair.Key))
		if err != nil {
			log.Errorf("[WatchJob] Unmarshal task value:%s error:%s", kvPair.Key, err)
			continue
		}
		w.taskChan <- TaskChange{Task: task, Action: 1}
	}
	watchStartRevision := resp.Header.Revision + 1
	// 在watchChan产生之前，job发生了增删，也会被感知到，进行同步
	watchChan := w.client.Watcher.Watch(ctx, w.taskPathPrefix, clientv3.WithPrefix(), clientv3.WithRev(watchStartRevision))
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
					task, err := ParseTaskFromKey(string(watchEvent.Kv.Key))
					if err != nil {
						log.Errorf("[WatchJob] Unmarshal task value:%s error:%s", watchEvent.Kv.Key, err)
						continue
					}
					w.Add(task)
				case mvccpb.DELETE:
					// 任务新建事件
					task, err := ParseTaskFromKey(string(watchEvent.Kv.Key))
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
func NewWorker(config WorkerConfig) (Worker, error) {
	err := config.Validation()
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
		config:         config,
		name:           name,
		closeChan:      make(chan struct{}),
		workerPath:     path.Join("/", config.RootPath, workerFolder),
		taskPathPrefix: path.Join(config.RootPath, taskFolder),
	}
	// 建立连接
	worker.client, err = clientv3.New(config.EtcdConfig)
	if err != nil {
		return nil, err
	}
	return &worker, nil
}

// input /klinefetch/job/ip:172.19.82.35-pid:23972/{"exchange":"ftx","type":"s","asset":"1INCH","to":"USD","symbol":"1INCH/USD","db":"1","onboard_date":"0001-01-01T00:00:00Z"}
// output ip:172.19.82.35-pid:23972.
func ParseWorkerKey(key string) string {
	arr := strings.SplitN(key, "/", 4)
	if len(arr) >= 4 {
		return arr[len(arr)-1]
	}
	return ""
}
