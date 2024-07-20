package escheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oleiade/lane/v2"
	"log/slog"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/stonever/balancer/balancer"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
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

type Master struct {
	Node
	ps     *pathParser
	ctx    context.Context
	cancel context.CancelFunc
	config MasterConfig

	// balancer
	RoundRobinBalancer balancer.Balancer
	HashBalancer       balancer.Balancer
	scheduleReqChan    chan string

	// path
	workerPath string
	coo        *Coordinator
	logger     *slog.Logger
}

func (m *Master) NotifySchedule(request string) {
	select {
	case m.scheduleReqChan <- request:
		m.logger.Debug("sent schedule request", "request", request)
	default:
		m.logger.Error("scheduler is too busy to handle task change request, ignored", "request", request)
	}
}

// NewMaster create a scheduler
func NewMaster(config MasterConfig, node Node) (*Master, error) {
	err := config.Validation()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to validate config")
	}
	err = node.Validate()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to validate node")
	}

	master := &Master{
		Node:            node,
		ps:              NewPathParser(node.RootName),
		config:          config,
		workerPath:      path.Join("/", node.RootName, workerFolder) + "/",
		scheduleReqChan: make(chan string, 1),
		logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{AddSource: true})).
			With("logger", "esched").With("master", node.Name),
		coo: NewCoordinator(1000),
	}
	master.ctx, master.cancel = context.WithCancel(context.Background())
	// pass worker's ctx to client
	node.EtcdConfig.Context = master.ctx
	// 建立连接
	master.client, err = clientv3.New(node.EtcdConfig)
	if err != nil {
		return nil, err
	}
	return master, nil
}

func (s *Master) ElectionKey() string {
	return path.Join("/"+s.RootName, electionFolder)
}

// Start The endless loop is for trying to election.
// lifecycle 1. if outer ctx done, scheduler done
// 2. if leader changed to the other, leader ctx done
func (m *Master) Start() {
	var (
		d = time.Second
	)
	for {
		err := m.Campaign(m.ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			m.logger.Error("failed to elect once, try again...", "error", err)
		}
		time.Sleep(d)
	}
}
func (m *Master) Stop() {
	m.cancel()
	m.client.Close()
}

// Campaign
//
//	@Description: 竞选期间有子context，其控制的生命周期包括
//	1. 成为leader 2. 监听workers的变动，通知chan进行调度 3.定时调度  3. 打印
//	@receiver s
//	@param ctx
//	@return error
func (m *Master) Campaign(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(err) // err 可能是nil也可能被赋值了

	// session的lease租约有效期设为30s，节点异常，最长等待15s，集群会产生新的leader执行调度
	session, err := concurrency.NewSession(m.client, concurrency.WithTTL(int(m.TTL)))
	if err != nil {
		m.logger.Error("failed to new session", "error", err)
		return err
	}
	defer session.Close()

	electionKey := m.ElectionKey()
	election := concurrency.NewElection(session, electionKey)
	leaderChange := election.Observe(ctx)

	// 竞选 Leader，直到成为 Leader 函数Campaign才返回
	err = election.Campaign(ctx, m.Name)
	if err != nil {
		m.logger.Error("failed to campaign", "error", err)
		return err
	}
	resp, err := election.Leader(ctx)
	if err != nil {
		m.logger.Error("failed to get leader", "error", err)
		return err
	}
	defer func() {
		_ = election.Resign(ctx)
	}()
	var leader string
	if len(resp.Kvs) > 0 {
		leader = string(resp.Kvs[0].Value)
	}
	m.logger.Info("become leader", "leader", leader)
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
			m.logger.Info("watch leader change", "leader:", newLeader)
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
func (s *Master) taskPath() string {
	return path.Join("/"+s.RootName, taskFolder)
}

func (m *Master) onlineWorkerList(ctx context.Context) (workersWithJob []string, err error) {
	resp, err := m.client.KV.Get(ctx, m.workerPath, clientv3.WithPrefix())
	if err != nil {
		return
	}
	workers := make([]string, 0, len(resp.Kvs))
	for _, kvPair := range resp.Kvs {
		worker, err := m.ps.parseWorkerIDFromWorkerKey(string(kvPair.Key))
		if err != nil {
			m.logger.Error("ParseWorkerFromWorkerKey error", "key", kvPair.Key, "error", err)
			continue
		}
		workers = append(workers, worker)
	}
	return workers, nil
}
func (s *Master) workerList(ctx context.Context) (workersWithJob map[string][]RawData, err error) {
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

func (m *Master) handleScheduleRequest(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			err := context.Cause(ctx)
			m.logger.Error("handleScheduleRequest exit, ctx done", "error", err)
			return err
		case reason := <-m.scheduleReqChan:
			if reason != ReasonFirstSchedule {
				m.logger.Info("doSchedule ", "wait", m.config.ReBalanceWait, "reason", reason)
				time.Sleep(m.config.ReBalanceWait)
			}
			func() {
				var (
					ctx    = ctx
					cancel context.CancelFunc
				)
				if m.config.Timeout > 0 {
					ctx, cancel = context.WithTimeout(ctx, m.config.Timeout)
					defer cancel()
				}
				err := m.doSchedule(ctx)
				if err != nil {
					m.logger.Error("schedule error,continue", "error", err)
					return
				}
				m.logger.Info("schedule done", "reason", reason)
			}()

		}
	}
}

func (m *Master) doSchedule(ctx context.Context) error {
	// query all online worker in etcd
	workerList, err := m.onlineWorkerList(ctx)
	if err != nil {
		m.logger.Error("failed to get leader", "error", err)
		return err
	}

	// start to assign
	generatedTaskList, err := m.config.Generator(ctx)
	if err != nil {
		err = errors.Wrapf(err, "failed to generate tasks")
		return err
	}
	m.logger.Info("coolect worker list and task list", "workerLen", len(workerList), "taskLen", len(generatedTaskList), "workerList", workerList)

	generatedTaskMap := make(map[string]Task)
	for _, task := range generatedTaskList {
		generatedTaskMap[task.ID] = task
	}

	if len(workerList) != m.MaxNumNodes-1 {
		m.logger.Warn("worker count not expected", "expected", m.MaxNumNodes-1)
	}
	// /20220809/task
	taskPathResp, err := m.client.KV.Get(ctx, m.taskPath(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	oldAssinMap, err := m.genOldAssignMap(taskPathResp.Kvs)
	if err != nil {
		return err
	}

	if len(workerList) <= 0 {
		return errors.New("worker count is zero")
	}

	toDeleteWorkerAllTask, toDeleteTask, toAddTask, err := m.coo.GetReBalanceResult(workerList, generatedTaskMap, oldAssinMap)
	if err != nil {
		return err
	}
	if len(toDeleteWorkerAllTask) > 0 {
		for _, workerName := range toDeleteWorkerAllTask {
			workerTaskFolderKey := m.ps.EncodeTaskFolderKey(workerName)
			deleted, err := m.client.KV.Delete(ctx, workerTaskFolderKey, clientv3.WithPrefix())
			if err != nil {
				return fmt.Errorf("failed to clear task. err:%w", err)
			}
			m.logger.Warn("workerTaskFolderKey deleted  ", "key", workerTaskFolderKey, "Deleted", deleted.Deleted)

		}
	}
	if len(toDeleteTask) > 0 {
		// get incremental tasks
		for workName, tasks := range toDeleteTask {
			if len(tasks) == 0 {
				continue
			}
			m.logger.Info("delete a worker's expired task", "workName", workName, "len", len(tasks))
			for _, taskID := range tasks {
				taskKey := m.ps.EncodeTaskKey(workName, taskID)
				_, err := m.client.KV.Delete(ctx, taskKey)
				if err != nil {
					return fmt.Errorf("failed to clear task. err:%w", err)
				}
			}
		}
	}
	total := 0
	priorityQueue := lane.NewMaxPriorityQueue[WorkerTask, float64]()
	for worker, arr := range toAddTask {
		for _, ids := range arr {
			taskObj := generatedTaskMap[ids]
			priorityQueue.Push(WorkerTask{Task: taskObj, worker: worker}, taskObj.P)
			total++
		}
	}
	var assignCount = 0

	for i := 0; i < total; i++ {
		taskWorker, _, ok := priorityQueue.Pop()
		if !ok {
			m.logger.Error("unexpected error while dequeue")
			break
		}
		taskObj := taskWorker.Task
		taskKey := m.ps.EncodeTaskKey(taskWorker.worker, taskObj.ID)
		data, err := json.Marshal(taskObj)
		if err != nil {
			return err
		}
		_, err = m.client.KV.Put(ctx, taskKey, string(data))
		if err != nil {
			return err
		}
		m.logger.Info("assign task", "taskKey", taskKey, "priority", taskObj.P, "worker", taskWorker.worker)
		assignCount++
	}
	if priorityQueue.Size() != 0 {
		m.logger.Error("priorityQueue should be empty", "actual", priorityQueue.Size())
	}

	m.logger.Info("ReBalance result", "toAddTask", toAddTask, "toDeleteWorkerTaskKey", toDeleteWorkerAllTask, "toDeleteTaskKey", toDeleteTask)
	return nil
}

func (m *Master) genOldAssignMap(taskPathResp []*mvccpb.KeyValue) (map[string][]string, error) {
	var (
		ret = make(map[string][]string)
	)
	for _, kvPair := range taskPathResp {
		// Assuming the pod name is kline-flow-65f9d64d4d-2k4kz, the workerKey is 2k4kz
		workerID, err := m.ps.parseWorkerIDFromTaskKey(string(kvPair.Key))
		if err != nil {
			return nil, err
		}
		taskID, err := m.ps.parseTaskIDFromTaskKey(string(kvPair.Key))
		if err != nil {
			return nil, err
		}
		ret[workerID] = append(ret[workerID], taskID)
	}
	return ret, nil
}

// watch :1. watch worker changed and notify schedule
// 2. periodically  notify schedule
func (m *Master) watchSchedule(ctx context.Context) error {
	key := GetWorkerBarrierLeftKey(m.RootName)
	resp, err := m.client.KV.Get(ctx, key)
	if err != nil {
		m.logger.Error("get barrier kv failed.", "error", err)
		return err
	}
	if len(resp.Kvs) > 0 {
		m.logger.Info("no need to gotoBarrier", "worker", m.Name, "barrier status left", resp.Kvs[0].String())
	} else {
		err := m.gotoBarrier(ctx)
		if err != nil {
			m.logger.Error("failed to gotoBarrier", "error", err)
		}
	}

	m.NotifySchedule(ReasonFirstSchedule)
	resp, err = m.client.KV.Get(ctx, m.workerPath, clientv3.WithPrefix())
	if err != nil {
		m.logger.Error("get worker job list failed.", "error", err)
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
			m.logger.Error("handleScheduleRequest exit, ctx done", "error", err)
			return err
		case <-period.C:
			m.NotifySchedule("periodic scheduling")
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

func (m *Master) gotoBarrier(ctx context.Context) error {
	barrierKey := GetWorkerBarrierName(m.RootName)
	m.logger.Info("try to enter the barrier", "barrier_key", barrierKey)
	session, err := concurrency.NewSession(m.client)
	if err != nil {
		m.logger.Error("failed to new session", "error", err)
		return err
	}
	b := recipe.NewDoubleBarrier(session, barrierKey, m.MaxNumNodes)
	m.logger.Info("scheduler waiting double Barrier", "num", m.MaxNumNodes)
	err = b.Enter()
	if err != nil {
		m.logger.Error("scheduler enter double Barrier error", "num", m.MaxNumNodes, "error", err)
		return err
	}
	m.logger.Info("scheduler enter double Barrier", "error", err)
	_ = session.Close()
	m.logger.Info("scheduler left double Barrier", "error", err)
	statusKey := GetWorkerBarrierLeftKey(m.RootName)
	txnResp, err := m.client.Txn(ctx).If(clientv3util.KeyMissing(statusKey)).Then(clientv3.OpPut(statusKey, time.Now().Format(time.RFC3339))).Commit()
	if err != nil {
		m.logger.Error("failed to set barrier left", "error", err)
		return err
	}
	m.logger.Info("scheduler set once schedule status done", "key", statusKey, "Succeeded", txnResp.Succeeded)
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
