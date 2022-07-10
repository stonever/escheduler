package escheduler

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/stonever/escheduler/log"
	"github.com/zehuamama/balancer/balancer"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/zap"
	"os"
	"path"
	"time"
)

type SchedulerConfig struct {
	// Interval configures interval of schedule task.
	// If Interval is <= 0, the default 60 seconds Interval will be used.
	Interval      time.Duration
	Generator     Generator
	ReBalanceWait time.Duration
}

func (sc SchedulerConfig) Validation() error {
	if sc.Interval == 0 {
		return errors.New("Interval is required")
	}
	if sc.Generator == nil {
		return errors.New("Generator is required")
	}
	return nil
}

type schedulerInstance struct {
	Node
	config          SchedulerConfig
	lease           clientv3.Lease // 用于操作租约
	closeChan       chan struct{}  //
	scheduleBarrier *recipe.Barrier
	name            string

	// balancer
	RoundRobinBalancer balancer.Balancer
	HashBalancer       balancer.Balancer
	scheduleReqChan    chan string

	// path
	workerPath string
}

func (s *schedulerInstance) NotifySchedule(request string) {
	select {
	case s.scheduleReqChan <- request:
		log.Info("sent schedule request", zap.String("request", request))
	default:
		log.Warn("scheduler is too busy to handle task change request, ignored", zap.String("request", request))
	}
}

// NewScheduler create a scheduler
func NewScheduler(config SchedulerConfig, node Node) (Scheduler, error) {
	err := config.Validation()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot give the name to scheduler")
	}

	ip, err := GetLocalIP()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot give the name to scheduler")
	}
	pid := os.Getpid()
	name := fmt.Sprintf("%s-%d", ip, pid)
	scheduler := schedulerInstance{
		Node:            node,
		config:          config,
		name:            name,
		closeChan:       make(chan struct{}),
		workerPath:      path.Join("/", node.RootName, workerFolder) + "/",
		scheduleReqChan: make(chan string, 1),
	}
	// 建立连接
	scheduler.client, err = clientv3.New(node.EtcdConfig)
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
	return path.Join("/"+s.RootName, electionFolder)
}

// Start The endless loop is for trying to election.
// lifecycle 1. if outer ctx done, scheduler done
// 2. if closed by outer or inner ,scheduler done
// if session down, will be closed by inner
func (s *schedulerInstance) Start(ctx context.Context) error {
	var (
		err error
	)
	ctx, cancel := context.WithCancel(ctx)
	defer s.client.Close()
	defer cancel()
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
			log.Error("failed to elect once, try again", zap.Error(err))
		}
		time.Sleep(time.Minute)
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
	session, err = concurrency.NewSession(s.client, concurrency.WithTTL(int(s.TTL)))
	if err != nil {
		log.Error("failed to new session,err:%s", zap.Error(err))
		return err
	}
	defer session.Close()
	electionKey := s.ElectionKey()
	election := concurrency.NewElection(session, electionKey)
	c := election.Observe(ctx)

	// 竞选 Leader，直到成为 Leader 函数Campaign才返回
	err = election.Campaign(ctx, s.name)
	if err != nil {
		log.Error("failed to campaign, err:%s", zap.Error(err))
		return err
	}
	resp, err := election.Leader(ctx)
	if err != nil {
		log.Error("failed to get leader", zap.Error(err))
		return err
	}
	defer election.Resign(ctx)
	var leader string
	if len(resp.Kvs) > 0 {
		leader = string(resp.Kvs[0].Value)
	}
	log.Info("got leader", zap.Any("leader", leader))
	var errC = make(chan error, 2)
	go func() {
		s.handleScheduleRequest(ctx)
		errC <- errors.New("handleScheduleRequest exit unexpected")
	}()
	go func() {
		s.watch(ctx)
		errC <- errors.New("watch exit unexpected")
	}()

	for {
		var (
			ok bool
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errC:
			return err
		case _, ok = <-c:
			if !ok {
				break
			}
			resp, err = election.Leader(ctx)
			if err != nil {
				log.Error("failed to get leader", zap.Error(err))
				return err
			}
			if len(resp.Kvs) > 0 {
				newLeader := string(resp.Kvs[0].Value)
				log.Info("query new leader", zap.String("leader", newLeader), zap.String("me", s.name))
				if newLeader != s.name {
					err = errors.New("leader has changed, is not me")
					return err
				}
			}
			continue
		}
		if !ok {
			break
		}
	}
	// It is no longer a leader

	return errors.New("leader is over")
}

type Generator func(ctx context.Context) ([]Task, error)

// taskPath return for example: /20220624/task
func (s *schedulerInstance) taskPath() string {
	return path.Join("/"+s.RootName, taskFolder)
}

func (s *schedulerInstance) onlineWorkerList(ctx context.Context) (workersWithJob []string, err error) {
	resp, err := s.client.KV.Get(ctx, s.workerPath, clientv3.WithPrefix())
	if err != nil {
		return
	}
	workers := make([]string, 0, len(resp.Kvs))
	for _, kvPair := range resp.Kvs {
		worker, err := ParseWorkerFromWorkerKey(string(kvPair.Key))
		if err != nil {
			log.Error("ParseWorkerFromWorkerKey error", zap.ByteString("key", kvPair.Key), zap.Error(err))
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
			log.Error("handleScheduleRequest exit, ctx done")
			return
		case reason := <-s.scheduleReqChan:
			if reason != ReasonFirstSchedule {
				log.Info("doSchedule wait", zap.Duration("wait", s.config.ReBalanceWait))
				time.Sleep(s.config.ReBalanceWait)
			}
			err := s.doSchedule(ctx)
			if err != nil {
				log.Error("doSchedule error", zap.Error(err))
				continue
			}
			if reason == ReasonFirstSchedule {
				//err = s.scheduleBarrier.Release()
				log.Info("FirstSchedule done")
			}
		}
	}
}

func (s *schedulerInstance) doSchedule(ctx context.Context) error {
	// start to assign
	taskList, err := s.config.Generator(ctx)
	if err != nil {
		log.Error("failed to generate all task", zap.Error(err))
		return err
	}
	log.Info("generated all tasks", zap.Int("count", len(taskList)))
	taskMap := make(map[string]Task)
	for _, task := range taskList {
		taskMap[task.Abbr] = task
	}
	// query all online worker in etcd
	workerList, err := s.onlineWorkerList(ctx)
	if err != nil {
		log.Error("failed to get leader, err:%s", zap.Error(err))
		return err
	}
	log.Info("worker total", zap.Int("count", len(workerList)), zap.Any("array", workerList))

	workerMap := make(map[string]struct{})
	for _, value := range workerList {
		workerMap[value] = struct{}{}
	}

	// delete task which is belonged to expired workers
	toDeleteTaskKey := make([]string, 0)
	toDeleteWorkerTaskKey := make(map[string]struct{}, 0)

	taskPathResp, err := s.client.KV.Get(ctx, s.taskPath(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if len(workerList) <= 0 {
		return errors.New("worker count is zero")
	}
	var (
		avgWorkLoad float64
		taskNotHash float64
	)
	for _, value := range taskMap {
		if len(value.Key) == 0 {
			taskNotHash++
		}
	}
	avgWorkLoad = taskNotHash / float64(len(workerList))
	hashBalancer, err := balancer.Build(balancer.IPHashBalancer, workerList)
	if err != nil {
		return err
	}
	leastLoadBalancer, err := balancer.Build(balancer.LeastLoadBalancer, workerList)
	if err != nil {
		return err
	}

	var stickyMap = make(map[string]float64)
	for _, kvPair := range taskPathResp.Kvs {

		workerKey := ParseWorkerFromTaskKey(string(kvPair.Key))
		_, ok := workerMap[workerKey]
		if !ok {
			parentPath := path.Dir(string(kvPair.Key))
			toDeleteWorkerTaskKey[parentPath] = struct{}{}
			continue
		}
		task, err := ParseTaskAbbrFromTaskKey(string(kvPair.Key))
		if err != nil {
			log.Info("delete task because failed to ParseTaskFromTaskKey", zap.String("task", string(kvPair.Key)))
			toDeleteTaskKey = append(toDeleteTaskKey, string(kvPair.Key))
			continue
		}
		taskObj, ok := taskMap[string(task)]
		if !ok {
			// the invalid task existed in valid worker, so delete it
			toDeleteTaskKey = append(toDeleteTaskKey, string(kvPair.Key))
			log.Info("delete task because the invalid task existed in valid worker", zap.String("task", string(kvPair.Key)))
		} else if avgWorkLoad > 0 && stickyMap[workerKey] > avgWorkLoad {
			// the valid task existed in valid worker, but worker workload is bigger than avg,  so delete it
			toDeleteTaskKey = append(toDeleteTaskKey, string(kvPair.Key))
			log.Info("delete task because the valid task existed in valid worker, but worker workload is bigger than avg,  so delete it", zap.String("task", string(kvPair.Key)), zap.Float64("load", stickyMap[workerKey]), zap.Float64("avg", avgWorkLoad))

		} else {
			// this valid task is existed in valid worker, so just do it, and give up being re-balance
			delete(taskMap, string(task))
			if len(taskObj.Key) == 0 {
				leastLoadBalancer.Inc(workerKey)
				stickyMap[workerKey]++
			}
		}
	}
	if len(toDeleteWorkerTaskKey) > 0 {
		log.Info("to delete expired worker's task folder", zap.Int("len", len(toDeleteWorkerTaskKey)))
		for prefix := range toDeleteWorkerTaskKey {
			_, err := s.client.KV.Delete(ctx, prefix, clientv3.WithPrefix())
			if err != nil {
				return fmt.Errorf("failed to clear task. err:%w", err)
			}
		}
	}
	if len(toDeleteTaskKey) > 0 {
		// get incremental tasks
		log.Info("to delete expired task ", zap.Int("len", len(toDeleteTaskKey)))
		for _, prefix := range toDeleteTaskKey {
			_, err := s.client.KV.Delete(ctx, prefix)
			if err != nil {
				return fmt.Errorf("failed to clear task. err:%w", err)
			}
		}
	}

	assignMap := make(map[string][]Task)
	for _, value := range taskMap {
		if len(value.Key) == 0 {
			assignTo, err := leastLoadBalancer.Balance(string(value.Abbr))
			if err != nil {
				return err
			}
			leastLoadBalancer.Inc(assignTo)
			assignMap[assignTo] = append(assignMap[assignTo], value)
			continue
		}
		assignTo, err := hashBalancer.Balance(value.Key)
		if err != nil {
			return err
		}
		assignMap[assignTo] = append(assignMap[assignTo], value)
	}
	var assignCount = 0

	for worker, arr := range assignMap {
		for _, value := range arr {
			taskKey := path.Join(s.taskPath(), worker, string(value.Abbr))
			_, err = s.client.KV.Put(ctx, taskKey, string(value.Raw))
			if err != nil {
				return err
			}
			assignCount++
		}

	}
	log.Info("task rebalance count", zap.Int("count", assignCount))

	return nil
}

// watch :1. watch worker changed and notify
// 2. periodic  notify
func (s *schedulerInstance) watch(ctx context.Context) {
	key := GetWorkerBarrierStatusKey(s.RootName)
	if resp, _ := s.client.KV.Get(ctx, key); len(resp.Kvs) > 0 {
		log.Info("no need to gotoBarrier", zap.String("worker", s.name), zap.String("barrier status", resp.Kvs[0].String()))
	} else {
		err := s.gotoBarrier(ctx)
		if err != nil {
			log.Error("failed to gotoBarrier", zap.Error(err))
		}
	}

	s.NotifySchedule(ReasonFirstSchedule)
	resp, err := s.client.KV.Get(ctx, s.workerPath, clientv3.WithPrefix())
	if err != nil {
		log.Error("get worker job list failed.", zap.Error(err))
		return
	}
	ticker := time.NewTicker(s.config.Interval)
	defer ticker.Stop()
	watchStartRevision := resp.Header.Revision + 1
	watchChan := s.client.Watcher.Watch(ctx, s.workerPath, clientv3.WithPrefix(), clientv3.WithRev(watchStartRevision))
	for {
		select {
		case <-ctx.Done():
			log.Error("watch exit, ctx done")
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
			}
		}
	}
}

func (s *schedulerInstance) gotoBarrier(ctx context.Context) error {
	key := GetWorkerBarrierName(s.RootName)
	session, err := concurrency.NewSession(s.client)
	if err != nil {
		log.Error("failed to new session", zap.Error(err))
		return err
	}
	b := recipe.NewDoubleBarrier(session, key, s.MaxNum)
	log.Info("scheduler waiting double Barrier", zap.String("scheduler", s.name), zap.Int("num", s.MaxNum))
	err = b.Enter()
	if err != nil {
		log.Error("scheduler enter double Barrier error", zap.String("scheduler", s.name), zap.Int("num", s.MaxNum), zap.Error(err))
		return err
	}
	log.Info("scheduler enter double Barrier", zap.String("scheduler", s.name), zap.Error(err))
	_ = session.Close()
	log.Info("scheduler left double Barrier", zap.String("scheduler", s.name), zap.Error(err))
	statusKey := GetWorkerBarrierStatusKey(s.RootName)
	txnResp, err := s.client.Txn(ctx).If(clientv3util.KeyMissing(statusKey)).Then(clientv3.OpPut(statusKey, "ReasonFirstSchedule")).Commit()
	if err != nil {
		log.Error("failed to set first scheduler", zap.Error(err))
		return err
	}
	log.Info("scheduler set once schedule status done", zap.String("key", statusKey), zap.Bool("Succeeded", txnResp.Succeeded))
	return nil
}
