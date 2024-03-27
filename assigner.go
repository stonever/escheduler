package escheduler

import (
	"log/slog"
	"sync"

	"github.com/elliotchance/pie/v2"
	"github.com/pkg/errors"
)

func NewAssigner() *Assigner {
	return &Assigner{
		ring: NewHashRing(10000, nil),
	}
}

type Assigner struct {
	sync.Mutex
	ring     *HashRing
	rootName string
	logger   *slog.Logger
}

func (a *Assigner) assignToRing(workerList []string, taskMap map[string]Task) (assignMap map[string][]string, err error) {
	a.ring.Reset(workerList...)
	assignMap = make(map[string][]string)
	for taskID, v := range taskMap {
		keyToRing := getKeyInRing(v)
		workerKey := a.ring.Get(keyToRing)
		if len(workerKey) == 0 {
			return nil, errors.Errorf("failed locate task:%s in ring", v.ID)
		}
		assignMap[workerKey] = append(assignMap[workerKey], taskID)
	}
	return assignMap, nil
}

// GetReBalanceResult
// workerList current online worker list, elements are worker's name
// taskMap current task collection, key is ID ,value is task
// taskPathResp current assigned state
// taskPathResp []kv key: /Root/task/worker-0/task-abbr-1 value: task raw data for task 1
func (a *Assigner) GetReBalanceResult(workers []string, generatedTaskMap map[string]Task, oldAssignMap map[string][]string) (toDeleteWorkerAllTask []string, toDeleteTask map[string][]string, toAddTask map[string][]string, err error) {
	_, toDeleteWorkerAllTask = pie.Diff[string](pie.Keys(oldAssignMap), workers)

	newAssignMap, err := a.assignToRing(workers, generatedTaskMap)
	if err != nil {
		return
	}
	toDeleteTask = make(map[string][]string, 0)
	toAddTask = make(map[string][]string, 0)

	onlineWorkerMap := make(map[string]struct{})
	for _, value := range workers {
		onlineWorkerMap[value] = struct{}{}
	}

	for _, workerKey := range workers {
		added, removed := pie.Diff(oldAssignMap[workerKey], newAssignMap[workerKey])
		toDeleteTask[workerKey] = append(toDeleteTask[workerKey], removed...)
		toAddTask[workerKey] = append(toAddTask[workerKey], added...)
	}
	return
}

func getKeyInRing(task Task) string {
	if task.Key != "" {
		return (task.Key)
	}
	return (task.ID)
}
