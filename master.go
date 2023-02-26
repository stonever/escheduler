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

type MasterConfig struct {
	// Interval configures interval of schedule task.
	// If Interval is <= 0, the default 60 seconds Interval will be used.
	Interval      time.Duration
	Timeout       time.Duration // The maximum time to schedule once
	Generator     Generator
	ReBalanceWait time.Duration
}

func (sc MasterConfig) Validation() error {
	if sc.Interval == 0 {
		return errors.New("Interval is required")
	}
	if sc.Generator == nil {
		return errors.New("Generator is required")
	}
	return nil
}

type master struct {
	Node
	ctx    context.Context
	cancel context.CancelFunc
	config MasterConfig

	// balancer
	RoundRobinBalancer balancer.Balancer
	HashBalancer       balancer.Balancer
	scheduleReqChan    chan string

	// path
	workerPath string
	assigner   Assigner
}

func (s *master) NotifySchedule(request string) {
	select {
	case s.scheduleReqChan <- request:
		log.Info("sent schedule request", zap.String("request", request))
	default:
		log.Error("scheduler is too busy to handle task change request, ignored", zap.String("request", request))
	}
}

// NewMaster create a scheduler
func NewMaster(config MasterConfig, node Node) (Master, error) {
	err := config.Validation()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to validate config")
	}
	err = node.Validation()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to validate node")
	}

	master := master{
		Node:            node,
		config:          config,
		workerPath:      path.Join("/", node.RootName, workerFolder) + "/",
		scheduleReqChan: make(chan string, 1),
	}
	master.assigner.rootName = master.RootName
	master.ctx, master.cancel = context.WithCancel(context.Background())
	// pass worker's ctx to client
	node.EtcdConfig.Context = master.ctx
	// 建立连接
	master.client, err = clientv3.New(node.EtcdConfig)
	if err != nil {
		return nil, err
	}
	return &master, nil
}

type Master interface {
	Start()
	NotifySchedule(string)
	Stop()
}

func (s *master) ElectionKey() string {
	return path.Join("/"+s.RootName, electionFolder)
}

// Start The endless loop is for trying to election.
// lifecycle 1. if outer ctx done, scheduler done
// 2. if leader changed to the other, leader ctx done
func (m *master) Start() {
	var (
		d = time.Second
	)
	for {
		err := m.Campaign(m.ctx)
		if err == context.Canceled {
			return
		}
		if err != nil {
			log.Error("failed to elect once, try again...", zap.Error(err))
		}
		time.Sleep(d)
	}
}
func (m *master) Stop() {
	m.cancel()
}

// Campaign
//
//	@Description: 竞选期间有子context，其控制的生命周期包括
//	1. 成为leader 2. 监听workers的变动，通知chan进行调度 3.定时调度  3. 打印
//	@receiver s
//	@param ctx
//	@return error
func (m *master) Campaign(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(err) // err 可能是nil也可能被赋值了

	// session的lease租约有效期设为30s，节点异常，最长等待15s，集群会产生新的leader执行调度
	session, err := concurrency.NewSession(m.client, concurrency.WithTTL(int(m.TTL)))
	if err != nil {
		log.Error("failed to new session,err:%s", zap.Error(err))
		return err
	}
	defer session.Close()

	electionKey := m.ElectionKey()
	election := concurrency.NewElection(session, electionKey)
	leaderChange := election.Observe(ctx)

	// 竞选 Leader，直到成为 Leader 函数Campaign才返回
	err = election.Campaign(ctx, m.Name)
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
	log.Info("become leader", zap.Any("leader", leader))
	// 成为leader后要履行leader的指责，决定发出调度请求以及进行调度
	go func() {
		err := m.handleScheduleRequest(ctx)
		cancel(err)
	}()
	go func() {
		err := m.watchSchedule(ctx)
		cancel(err)
	}()

	for {
		select {
		case <-ctx.Done():
			err := context.Cause(ctx)
			log.Error("Campaign exit, ctx done", zap.Error(err))
			return err
		case resp, ok := <-leaderChange:
			// 如果not ok表示leaderChange这个channel关闭，那么即使leader变了也不知道，所以退出重试
			if !ok {
				return errors.New("chan leaderChange is closed")
			}
			if len(resp.Kvs) == 0 {
				break
			}

			newLeader := string(resp.Kvs[0].Value)
			log.Info("watch leader change", zap.String("leader:", newLeader))
			if newLeader != m.Name {
				// It is no longer a leader
				err = errors.Errorf("leader has changed to %s, not %s", newLeader, m.Name)
				return err
			}

			continue
		}
	}
}

type Generator func(ctx context.Context) ([]Task, error)

// taskPath return for example: /20220624/task
func (s *master) taskPath() string {
	return path.Join("/"+s.RootName, taskFolder)
}

func (m *master) onlineWorkerList(ctx context.Context) (workersWithJob []string, err error) {
	resp, err := m.client.KV.Get(ctx, m.workerPath, clientv3.WithPrefix())
	if err != nil {
		return
	}
	workers := make([]string, 0, len(resp.Kvs))
	for _, kvPair := range resp.Kvs {
		worker, err := ParseWorkerIDFromWorkerKey(m.RootName, string(kvPair.Key))
		if err != nil {
			log.Error("ParseWorkerFromWorkerKey error", zap.ByteString("key", kvPair.Key), zap.Error(err))
			continue
		}
		workers = append(workers, worker)
	}
	return workers, nil
}
func (s *master) workerList(ctx context.Context) (workersWithJob map[string][]RawData, err error) {
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

func (m *master) handleScheduleRequest(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			err := context.Cause(ctx)
			log.Error("handleScheduleRequest exit, ctx done", zap.Error(err))
			return err
		case reason := <-m.scheduleReqChan:
			if reason != ReasonFirstSchedule {
				log.Info("doSchedule wait", zap.Duration("wait", m.config.ReBalanceWait))
				time.Sleep(m.config.ReBalanceWait)
			}
			ctx := ctx
			if m.config.Timeout > 0 {
				ctx, _ = context.WithTimeout(ctx, m.config.Timeout)
			}
			err := m.doSchedule(ctx)
			if err != nil {
				log.Error("schedule error,continue", zap.Error(err))
				continue
			}
			log.Info("schedule done", zap.String("reason", reason))
		}
	}
}

func (s *master) doSchedule(ctx context.Context) error {
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

// watch :1. watch worker changed and notify schedule
// 2. periodically  notify schedule
func (m *master) watchSchedule(ctx context.Context) error {
	key := GetWorkerBarrierLeftKey(m.RootName)
	if resp, _ := m.client.KV.Get(ctx, key); len(resp.Kvs) > 0 {
		log.Info("no need to gotoBarrier", zap.String("worker", m.Name), zap.String("barrier status left", resp.Kvs[0].String()))
	} else {
		err := m.gotoBarrier(ctx)
		if err != nil {
			log.Error("failed to gotoBarrier", zap.Error(err))
		}
	}

	m.NotifySchedule(ReasonFirstSchedule)
	resp, err := m.client.KV.Get(ctx, m.workerPath, clientv3.WithPrefix())
	if err != nil {
		log.Error("get worker job list failed.", zap.Error(err))
		return err
	}
	period := time.NewTicker(m.config.Interval)
	defer period.Stop()
	watchStartRevision := resp.Header.Revision + 1
	watchChan := m.client.Watch(ctx, m.workerPath, clientv3.WithPrefix(), clientv3.WithRev(watchStartRevision))
	for {
		select {
		case <-ctx.Done():
			err := context.Cause(ctx)
			log.Error("handleScheduleRequest exit, ctx done", zap.Error(err))
			return err
		case <-period.C:
			m.NotifySchedule("periodic task scheduling ")
		case watchResp := <-watchChan:
			for _, watchEvent := range watchResp.Events {
				if watchEvent.IsCreate() {
					m.NotifySchedule(fmt.Sprintf("create worker:%s ", watchEvent.Kv.Key))
					continue
				}
				if watchEvent.Type == mvccpb.DELETE {
					m.NotifySchedule(fmt.Sprintf("delete worker:%s ", watchEvent.Kv.Key))
					continue
				}
			}
		}
	}
}

func (m *master) gotoBarrier(ctx context.Context) error {
	key := GetWorkerBarrierName(m.RootName)
	session, err := concurrency.NewSession(m.client)
	if err != nil {
		log.Error("failed to new session", zap.Error(err))
		return err
	}
	b := recipe.NewDoubleBarrier(session, key, m.MaxNum)
	log.Info("scheduler waiting double Barrier", zap.String("scheduler", m.Name), zap.Int("num", m.MaxNum))
	err = b.Enter()
	if err != nil {
		log.Error("scheduler enter double Barrier error", zap.String("scheduler", m.Name), zap.Int("num", m.MaxNum), zap.Error(err))
		return err
	}
	log.Info("scheduler enter double Barrier", zap.String("scheduler", m.Name), zap.Error(err))
	_ = session.Close()
	log.Info("scheduler left double Barrier", zap.String("scheduler", m.Name), zap.Error(err))
	statusKey := GetWorkerBarrierLeftKey(m.RootName)
	txnResp, err := m.client.Txn(ctx).If(clientv3util.KeyMissing(statusKey)).Then(clientv3.OpPut(statusKey, time.Now().Format(time.RFC3339))).Commit()
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
