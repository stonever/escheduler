package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/zehuamama/balancer/balancer"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"os"
	"path"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)
}

type JobConfig struct {
	Generator func() ([]Task, error)
	// Interval configures interval of schedule task.
	// If Interval is <= 0, the default 60 seconds Interval will be used.
	Interval time.Duration
}
type SchedulerConfig struct {
	JobConfig
	EtcdConfig clientv3.Config
	RootName   string
	// TTL configures the session's TTL in seconds.
	// If TTL is <= 0, the default 60 seconds TTL will be used.
	NodeTTL int
}

func (sc SchedulerConfig) Validation() error {
	if len(sc.RootName) == 0 {
		return errors.New("RootName is required")
	}
	if sc.Interval == 0 {
		return errors.New("Interval is required")
	}
	return nil
}

type schedulerInstance struct {
	config    SchedulerConfig
	taskGen   taskGen
	client    *clientv3.Client
	lease     clientv3.Lease // 用于操作租约
	closeChan chan struct{}  //

	name         string
	distribution chan workerChange

	// balancer
	RoundRobinBalancer balancer.Balancer
	HashBalancer       balancer.Balancer
	scheduleReqChan    chan string

	// path
	workerPath string
}

func (s *schedulerInstance) NotifySchedule(reason string) {
	s.scheduleReqChan <- reason
}

// NewScheduler create a scheduler
func NewScheduler(config SchedulerConfig, tg taskGen) (Scheduler, error) {
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
	scheduler := schedulerInstance{
		config:          config,
		name:            name,
		distribution:    make(chan workerChange, 1),
		taskGen:         tg,
		closeChan:       make(chan struct{}),
		workerPath:      path.Join("/", config.RootName, workerFolder),
		scheduleReqChan: make(chan string, 0),
	}
	// 建立连接
	scheduler.client, err = clientv3.New(config.EtcdConfig)
	if err != nil {
		return nil, err
	}
	return &scheduler, nil
}

func PeriodSchedule(ctx context.Context, interval time.Duration) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ctx.Done():
				return errors.Wrapf(ctx.Err(), "PeriodSchedule exit")
			case <-ticker.C:

			}
		}
	}

}

type Scheduler interface {
	Start(ctx context.Context) error
	NotifySchedule(string)
	Stop()
}

var ErrSchedulerClosed = errors.New("scheduler was closed")

func (s *schedulerInstance) ElectionKey() string {
	u, _ := uuid.NewUUID()
	return path.Join("/"+s.config.RootName, electionFolder, u.String())
}

// Start The endless loop is for trying to election.
// lifecycle 1. if outer ctx done, scheduler done
// 2. if closed by outer or inner ,scheduler done
// if session down, will be closed by inner
func (s *schedulerInstance) Start(ctx context.Context) error {
	var (
		err error
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closeChan:
			return ErrSchedulerClosed
		default:
		}
		err = s.ElectOnce(ctx)
		if err != nil {
			log.Errorf("failed to elect once,err:%s", err)
		}
	}

}
func (s *schedulerInstance) Stop() {
	close(s.closeChan)
}
func (s *schedulerInstance) ElectOnce(ctx context.Context) error {
	var (
		session *concurrency.Session
		err     error
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// session的lease租约有效期设为30s，节点异常，最长等待15s，集群会产生新的leader执行调度
	session, err = concurrency.NewSession(s.client, concurrency.WithTTL(s.config.NodeTTL))
	if err != nil {
		logger.Errorf("failed to new session,err:%s", err)
		return err
	}
	defer session.Close()
	electionKey := s.ElectionKey()
	election := concurrency.NewElection(session, electionKey)
	log.Infof("try to elect %s", electionKey)
	// 竞选 Leader，直到成为 Leader 函数Campaign才返回
	err = election.Campaign(ctx, s.name)
	if err != nil {
		logger.Errorf("failed to campaign, err:%s", err)
		return err
	}
	resp, err := election.Leader(ctx)
	if err != nil {
		logger.Errorf("failed to get leader, err:%s", err)
		return err
	}
	defer election.Resign(ctx)
	log.Infof("got leader %+v", resp)
	go s.handleScheduleRequest(ctx)
	go s.watch(ctx)
	c := election.Observe(ctx)
	for {
		var (
			ok           bool
			electionResp clientv3.GetResponse
		)
		select {
		case electionResp, ok = <-c:
			if !ok {
				break
			}
			log.Infof("watch election got response :%v %t", electionResp, ok)
			continue
		}
		if !ok {
			break
		}
	}
	// It is no longer a leader
	return errors.New("leader is over")
}

type taskGen func(ctx context.Context) ([]Task, error)

type workerChange struct {
	Action int    // 1: add 2: delete
	Worker string // worker name
}

func (s *schedulerInstance) taskPath() string {
	return path.Join("/"+s.config.RootName, taskFolder)
}

func (s *schedulerInstance) onlineWorkerList(ctx context.Context) (workersWithJob []string, err error) {
	resp, err := s.client.KV.Get(ctx, s.workerPath, clientv3.WithPrefix())
	if err != nil {
		return
	}
	workers := make([]string, 0, len(resp.Kvs))
	for _, kvPair := range resp.Kvs {
		worker, err := ParseWorkerFromKey(string(kvPair.Key))
		if err != nil {
			log.Errorf("ParseWorkerFromKey for %s error :%s", kvPair.Key, err)
			continue
		}
		workers = append(workers, worker)
	}
	return workers, nil
}
func (s *schedulerInstance) workerList(ctx context.Context) (workersWithJob map[string][]RawData, err error) {
	workersWithJob = make(map[string][]RawData)
	resp, err := s.client.KV.Get(ctx, s.workerPath, clientv3.WithPrefix())
	if err != nil {
		return
	}
	for _, kvPair := range resp.Kvs {
		workerName := path.Base(string(kvPair.Key))
		workersWithJob[workerName] = make([]RawData, 0)
	}
	for key := range workersWithJob {
		resp, err = s.client.KV.Get(ctx, key, clientv3.WithPrefix())
		if err != nil {
			return
		}
		for _, kvPair := range resp.Kvs {
			workersWithJob[key] = append(workersWithJob[key], kvPair.Value)
		}
	}
	return workersWithJob, nil
}

func (s *schedulerInstance) handleScheduleRequest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.scheduleReqChan:
			err := s.doSchedule(ctx)
			if err != nil {
				log.Errorf("doSchedule error: %s", err)
			}
		}
	}
}

func (s *schedulerInstance) doSchedule(ctx context.Context) error {
	// start to assign
	taskList, err := s.taskGen(ctx)
	if err != nil {
		logger.Errorf("failed to get leader, err:%s", err)
		return err
	}
	taskMap := make(map[string]Task)
	for _, task := range taskList {
		taskMap[string(task.Raw)] = task
	}
	// query all online worker in etcd
	workerList, err := s.onlineWorkerList(ctx)
	if err != nil {
		logger.Errorf("failed to get leader, err:%s", err)
		return err
	}
	workerMap := make(map[string]struct{})
	for _, value := range workerList {
		workerMap[value] = struct{}{}
	}
	// delete expired workers
	workerPathResp, err := s.client.KV.Get(ctx, s.workerPath, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kvPair := range workerPathResp.Kvs {
		// kvPair.Key形如/klinefetch/job/ip:172.19.82.35-pid:10888/{"exchange":"binance","assert":"BTC","symbol":"BTCBKRW"}
		// path.Dir(string(kvPair.Key))取出/klinefetch/job/ip:172.19.82.35-pid:10888
		// path.Base取路径的最后一个元素，取出ip:172.19.82.35-pid:10888
		workerKey := ParseWorkerKey(string(kvPair.Key))
		_, ok := workerMap[workerKey]
		if ok {
			continue
		}
		_, err := s.client.KV.Delete(ctx, string(kvPair.Key), clientv3.WithPrefix())
		if err != nil {
			return fmt.Errorf("failed to clear task. err:%w", err)
		}
		_, err = s.client.KV.Delete(ctx, s.taskPath(), clientv3.WithPrefix())
		if err != nil {
			return fmt.Errorf("failed to clear task. err:%w", err)
		}
	}
	toDeleteTaskKey := make([]string, 0)
	taskPathResp, err := s.client.KV.Get(ctx, s.taskPath(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kvPair := range taskPathResp.Kvs {

		workerKey := ParseWorkerKey(string(kvPair.Key))
		_, ok := workerMap[workerKey]
		if !ok {
			toDeleteTaskKey = append(toDeleteTaskKey, string(kvPair.Key))
			continue
		}
		_, ok = taskMap[string(kvPair.Value)]
		if !ok {
			toDeleteTaskKey = append(toDeleteTaskKey, string(kvPair.Value))
		} else {
			delete(taskMap, string(kvPair.Value))
		}
	}

	if len(toDeleteTaskKey) > 0 {

		// get incremental tasks
		log.Infof("to delete expired task len:%d %v\n", len(toDeleteTaskKey), toDeleteTaskKey)
		for _, prefix := range toDeleteTaskKey {
			deleteResp, err := s.client.KV.Delete(ctx, prefix, clientv3.WithPrefix())
			if err != nil {
				log.Errorf("task belong to %s total deleted:%d", prefix, deleteResp.Deleted)
				return fmt.Errorf("failed to clear task. err:%w", err)
			}
			log.Infof("task belong to %s total deleted:%d", prefix, deleteResp.Deleted)
		}
	}
	hashBalancer, err := balancer.Build(balancer.IPHashBalancer, workerList)
	if err != nil {
		return err
	}
	leastLoadBalancer, err := balancer.Build(balancer.LeastLoadBalancer, workerList)
	if err != nil {
		return err
	}
	assignMap := make(map[string][]Task)
	for _, value := range taskMap {
		if len(value.Key) == 0 {
			assignTo, err := leastLoadBalancer.Balance(string(value.Raw))
			if err != nil {
				return err
			}
			assignMap[assignTo] = append(assignMap[assignTo], value)
			continue
		}
		assignTo, err := hashBalancer.Balance(value.Key)
		if err != nil {
			return err
		}
		assignMap[assignTo] = append(assignMap[assignTo], value)
	}
	for worker, arr := range assignMap {
		for _, value := range arr {
			taskKey := path.Join(s.taskPath(), worker, string(value.Raw))
			_, err = s.client.KV.Put(ctx, taskKey, "")
			if err != nil {
				return err
			}
		}

	}
	return nil
}

// watch :1. watch worker changed and notify
// 2. periodic  notify
func (s *schedulerInstance) watch(ctx context.Context) {
	s.NotifySchedule(fmt.Sprintf("first init"))
	resp, err := s.client.KV.Get(ctx, s.workerPath, clientv3.WithPrefix())
	if err != nil {
		log.Errorf("get worker job list failed. %w", err)
		s.Stop()
	}
	ticker := time.NewTicker(s.config.Interval)
	defer ticker.Stop()
	watchStartRevision := resp.Header.Revision + 1
	watchChan := s.client.Watcher.Watch(ctx, s.workerPath, clientv3.WithPrefix(), clientv3.WithRev(watchStartRevision))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.NotifySchedule("periodic task scheduling ")
		case watchResp := <-watchChan:
			for _, watchEvent := range watchResp.Events {
				if watchEvent.IsCreate() {
					s.NotifySchedule(fmt.Sprintf("create worker:%s ", watchEvent.Kv.Key))
					continue
				}
				if watchEvent.Type == mvccpb.DELETE {
					s.NotifySchedule(fmt.Sprintf("delete worker:%s ", watchEvent.Kv.Key))
					continue
				}
				log.Infof("ignore event:%v", watchEvent)
			}
		}

	}
}
