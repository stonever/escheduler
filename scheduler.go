package escheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/stonever/balancer/balancer"
	"github.com/stonever/escheduler/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/zap"
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
	ctx             context.Context
	cancel          context.CancelFunc
	config          SchedulerConfig
	lease           clientv3.Lease // 用于操作租约
	scheduleBarrier *recipe.Barrier

	// balancer
	RoundRobinBalancer balancer.Balancer
	HashBalancer       balancer.Balancer
	scheduleReqChan    chan string

	// path
	workerPath string
	assigner   Assigner
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
		return nil, errors.Wrapf(err, "failed to validate config")
	}
	err = node.Validation()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to validate node")
	}

	scheduler := schedulerInstance{
		Node:            node,
		config:          config,
		workerPath:      path.Join("/", node.RootName, workerFolder) + "/",
		scheduleReqChan: make(chan string, 1),
	}
	scheduler.ctx, scheduler.cancel = context.WithCancel(context.Background())
	// 建立连接
	scheduler.client, err = clientv3.New(node.EtcdConfig)
	if err != nil {
		return nil, err
	}
	return &scheduler, nil
}

type Scheduler interface {
	Start()
	NotifySchedule(string)
	Stop()
}

func (s *schedulerInstance) ElectionKey() string {
	return path.Join("/"+s.RootName, electionFolder)
}

// Start The endless loop is for trying to election.
// lifecycle 1. if outer ctx done, scheduler done
// 2. if closed by outer or inner ,scheduler done
// if session down, will be closed by inner
func (s *schedulerInstance) Start() {
	var (
		ctx = s.ctx
		d   = time.Minute
	)
	for {
		err := s.ElectOnce(ctx)
		if err == context.Canceled {
			return
		}
		if err != nil {
			log.Error("failed to elect once, try again...", zap.Error(err))
		}
		time.Sleep(d)
	}
}
func (s *schedulerInstance) Stop() {
	s.cancel()
	s.client.Close()
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
	defer func() {
		_ = session.Close()
	}()
	electionKey := s.ElectionKey()
	election := concurrency.NewElection(session, electionKey)
	c := election.Observe(ctx)

	// 竞选 Leader，直到成为 Leader 函数Campaign才返回
	err = election.Campaign(ctx, s.Name)
	if err != nil {
		log.Error("failed to campaign, err:%s", zap.Error(err))
		return err
	}
	resp, err := election.Leader(ctx)
	if err != nil {
		log.Error("failed to get leader", zap.Error(err))
		return err
	}
	defer func() {
		_ = election.Resign(ctx)
	}()
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
				log.Info("query new leader", zap.String("leader", newLeader), zap.String("me", s.Name))
				if newLeader != s.Name {
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
				log.Error("doSchedule error,continue", zap.Error(err))
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
		err = errors.Wrapf(err, "failed to generate tasks")
		return err
	}
	log.Info("succeeded to generate all tasks", zap.Int("count", len(taskList)))
	taskMap := make(map[string]Task)
	for _, task := range taskList {
		taskMap[task.ID] = task
	}
	// query all online worker in etcd
	workerList, err := s.onlineWorkerList(ctx)
	if err != nil {
		log.Error("failed to get leader, err:%s", zap.Error(err))
		return err
	}
	log.Info("worker total", zap.Int("count", len(workerList)), zap.Any("array", workerList))
	if len(workerList) != s.MaxNum-1 {
		log.Info("worker count not expected", zap.Int("expected", s.MaxNum-1))
	}
	// /20220809/task
	taskPathResp, err := s.client.KV.Get(ctx, s.taskPath(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if len(workerList) <= 0 {
		return errors.New("worker count is zero")
	}
	log.Info("rebalance workerList", zap.Strings("workerList", workerList))

	toDeleteWorkerTaskKey, toDeleteTaskKey, assignMap, err := s.assigner.GetReBalanceResult(workerList, taskMap, taskPathResp.Kvs)
	if err != nil {
		return err
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

	var assignCount = 0
	for worker, arr := range assignMap {
		for _, taskObj := range arr {
			taskKey := path.Join(s.taskPath(), worker, taskObj.ID)
			data, err := json.Marshal(taskObj)
			if err != nil {
				return err
			}
			_, err = s.client.KV.Put(ctx, taskKey, string(data))
			if err != nil {
				return err
			}
			assignCount++
		}

	}
	log.Info("task re-balance result", zap.Int("created count", assignCount), zap.Any("toDeleteWorkerTaskKey", toDeleteWorkerTaskKey), zap.Any("toDeleteTaskKey", toDeleteTaskKey))
	return nil
}

// watch :1. watch worker changed and notify
// 2. periodic  notify
func (s *schedulerInstance) watch(ctx context.Context) {
	key := GetWorkerBarrierLeftKey(s.RootName)
	if resp, _ := s.client.KV.Get(ctx, key); len(resp.Kvs) > 0 {
		log.Info("no need to gotoBarrier", zap.String("worker", s.Name), zap.String("barrier status left", resp.Kvs[0].String()))
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
	log.Info("scheduler waiting double Barrier", zap.String("scheduler", s.Name), zap.Int("num", s.MaxNum))
	err = b.Enter()
	if err != nil {
		log.Error("scheduler enter double Barrier error", zap.String("scheduler", s.Name), zap.Int("num", s.MaxNum), zap.Error(err))
		return err
	}
	log.Info("scheduler enter double Barrier", zap.String("scheduler", s.Name), zap.Error(err))
	_ = session.Close()
	log.Info("scheduler left double Barrier", zap.String("scheduler", s.Name), zap.Error(err))
	statusKey := GetWorkerBarrierLeftKey(s.RootName)
	txnResp, err := s.client.Txn(ctx).If(clientv3util.KeyMissing(statusKey)).Then(clientv3.OpPut(statusKey, time.Now().Format(time.RFC3339))).Commit()
	if err != nil {
		log.Error("failed to set barrier left", zap.Error(err))
		return err
	}
	log.Info("scheduler set once schedule status done", zap.String("key", statusKey), zap.Bool("Succeeded", txnResp.Succeeded))
	return nil
}

// randShuffle Shuffle the order of input slice, then return a new slice without changing the input
// 使用平均分配策略时，每个group的余数总是会优先分配到第一个worker，如果有n个group，余数都是1,那么first worker会多承担n个任务，这里通过打乱worker的顺序，使余数中的n个任务随机散落在等价的worker中
func randShuffle[T any](slice []T) []T {
	var newSlice = make([]T, len(slice))
	copy(newSlice, slice)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(newSlice), func(i, j int) {
		newSlice[i], newSlice[j] = newSlice[j], newSlice[i]
	})
	return newSlice
}
