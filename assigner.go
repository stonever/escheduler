package escheduler

import (
	"github.com/stonever/balancer/balancer"
	"github.com/stonever/escheduler/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
	"path"
	"sync"
)

type Assigner struct {
	sync.Mutex
	workerList   []string
	hashBalancer balancer.Balancer
	leastLoadMap map[string]balancer.Balancer
}

func (a *Assigner) getRandWorkers() []string {
	return randShuffle[string](a.workerList)
}
func (a *Assigner) GetBalancer(key, group string) (balancer.Balancer, error) {
	a.Lock()
	defer a.Unlock()
	if len(key) != 0 {
		return a.getHashBalancer()
	}

	return a.getLeastLoadBalancer(group)

}
func (a *Assigner) getHashBalancer() (balancer.Balancer, error) {
	var err error
	if a.hashBalancer == nil {
		a.hashBalancer, err = balancer.Build(balancer.IPHashBalancer, a.getRandWorkers())
		if err != nil {
			return nil, err
		}
	}
	return a.hashBalancer, nil
}
func (a *Assigner) getLeastLoadBalancer(group string) (balancer.Balancer, error) {
	ret, ok := a.leastLoadMap[group]
	if ok {
		return ret, nil
	}
	if a.leastLoadMap == nil {
		a.leastLoadMap = make(map[string]balancer.Balancer)
	}
	var err error
	a.leastLoadMap[group], err = balancer.Build(balancer.LeastLoadBalancer, a.getRandWorkers())
	if err != nil {
		return nil, err
	}
	return a.leastLoadMap[group], nil
}
func (a *Assigner) getWorkerLoadByGroup(group, worker string) uint64 {
	b, err := a.getLeastLoadBalancer(group)
	if err != nil {
		return 0
	}
	l, ok := b.(*balancer.LeastLoad)
	if !ok {
		return 0
	}
	return l.Value(worker)
}

// GetReBalanceResult
// workerList current online worker list, elements are worker's name
// taskMap current task collection, key is ID ,value is task
// taskPathResp current assigned state
// taskPathResp []kv key: /Root/task/worker-0/task-abbr-1 value: task raw data for task 1
func (a *Assigner) GetReBalanceResult(workerList []string, taskMap map[string]Task, taskPathResp []*mvccpb.KeyValue) (toDeleteWorkerTaskKey map[string]struct{}, toDeleteTaskKey []string, assignMap map[string][]Task, err error) {
	changed := a.reset(workerList)
	log.Info("assigner update workerList", zap.Bool("changed", changed))

	var (
		leastLoadBalancer, hashBalancer balancer.Balancer
	)
	// make return params
	toDeleteTaskKey = make([]string, 0)
	toDeleteWorkerTaskKey = make(map[string]struct{}, 0)
	assignMap = make(map[string][]Task)

	workerMap := make(map[string]struct{})
	for _, value := range workerList {
		workerMap[value] = struct{}{}
	}
	var (
		taskNotHash = make(map[string]float64)
	)
	for _, value := range taskMap {
		if len(value.Key) == 0 {
			taskNotHash[value.Group]++
		}
	}

	for _, kvPair := range taskPathResp {

		workerKey := ParseWorkerFromTaskKey(string(kvPair.Key))
		_, ok := workerMap[workerKey]
		if !ok {
			parentPath := path.Dir(string(kvPair.Key))
			toDeleteWorkerTaskKey[parentPath] = struct{}{}
			continue
		}
		var task string
		task, err = ParseTaskAbbrFromTaskKey(string(kvPair.Key))
		if err != nil {
			log.Info("delete task because failed to ParseTaskFromTaskKey", zap.String("task", string(kvPair.Key)))
			toDeleteTaskKey = append(toDeleteTaskKey, string(kvPair.Key))
			continue
		}
		log.Info("taskNotHash group", zap.Any("group", taskNotHash))
		taskObj, ok := taskMap[string(task)]
		if !ok {
			// the invalid task existed in valid worker, so delete it
			toDeleteTaskKey = append(toDeleteTaskKey, string(kvPair.Key))
			log.Info("delete task because the invalid task existed in valid worker", zap.String("task", string(kvPair.Key)))
			continue
		}
		avgWorkLoad := taskNotHash[taskObj.Group] / float64(len(workerList))
		stickyLoad := a.getWorkerLoadByGroup(taskObj.Group, workerKey)
		if avgWorkLoad > 0 && float64(stickyLoad)-avgWorkLoad > 0 {
			// the valid task existed in valid worker, but worker workload is bigger than avg,  so delete it
			toDeleteTaskKey = append(toDeleteTaskKey, string(kvPair.Key))
			log.Info("delete task because the valid task existed in valid worker, but worker workload is bigger than avg,  so delete it", zap.String("task", string(kvPair.Key)), zap.Uint64("load", stickyLoad), zap.Float64("avg", avgWorkLoad))
		} else {
			// this valid task is existed in valid worker, so just do it, and give up being re-balance
			delete(taskMap, string(task))
			if len(taskObj.Key) == 0 {
				if taskObj.Group == "C" {
					log.Info("")
				}
				leastLoadBalancer, err = a.GetBalancer(taskObj.Key, taskObj.Group)
				if err != nil {
					return
				}
				leastLoadBalancer.Inc(workerKey)
			}
		}
	}
	// taskMap is all tasks, key is task's ID ,value is task
	for _, taskObj := range taskMap {
		if taskObj.Group == "C" {
			log.Info("")
		}
		var workerKey string
		if len(taskObj.Key) == 0 {
			leastLoadBalancer, err = a.GetBalancer(taskObj.Key, taskObj.Group)
			if err != nil {
				return
			}

			workerKey, err = leastLoadBalancer.Balance(string(taskObj.ID))
			if err != nil {
				return
			}
			log.Info("task assign to worker", zap.String("group", taskObj.Group), zap.String("worker", workerKey))

			leastLoadBalancer.Inc(workerKey)
			assignMap[workerKey] = append(assignMap[workerKey], taskObj)
			continue
		}
		hashBalancer, err = a.GetBalancer(taskObj.Key, taskObj.Group)
		if err != nil {
			return
		}
		workerKey, err = hashBalancer.Balance(taskObj.Key)
		if err != nil {
			return
		}
		assignMap[workerKey] = append(assignMap[workerKey], taskObj)
	}
	return
}

func (a *Assigner) reset(list []string) bool {
	a.Lock()
	defer a.Unlock()

	a.workerList = list
	a.leastLoadMap = nil
	a.hashBalancer = nil
	return true
}
