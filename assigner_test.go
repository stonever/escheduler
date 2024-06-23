package escheduler

import (
	"fmt"
	"testing"
)

func TestAssigner_GetReBalanceResult(t *testing.T) {
	var (
		a = NewCoordinator(1)
	)
	workerList := make([]string, 0)
	for i := 0; i < 10; i++ {
		workerList = append(workerList, fmt.Sprintf("worker-%d", i))
	}
	taskMap := make(map[string]Task)
	for i := 0; i < 10; i++ {
		taskMap[fmt.Sprintf("%d", i)] = Task{ID: fmt.Sprintf("%d", i)}
	}
	var oldAssignMap map[string][]string
	t.Log("first assign")
	gotToDeleteWorkerKey, gotToDeleteTaskKey, gotToAssignMap, err := a.GetReBalanceResult(workerList, taskMap, oldAssignMap)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ToDeleteWorkerTaskKey:", gotToDeleteWorkerKey)
	t.Log("ToDeleteTaskKey:", gotToDeleteTaskKey)
	t.Log("ToAssignMap:", gotToAssignMap)
	t.Log("second assign")
	workerList = workerList[5:]
	oldAssignMap = gotToAssignMap
	gotToDeleteWorkerKey, gotToDeleteTaskKey, gotToAssignMap, err = a.GetReBalanceResult(workerList, taskMap, oldAssignMap)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ToDeleteWorkerTaskKey:", gotToDeleteWorkerKey)
	t.Log("ToDeleteTaskKey:", gotToDeleteTaskKey)
	t.Log("ToAssignMap:", gotToAssignMap)
	t.Log("third assign")
	for i := 10; i < 15; i++ {
		workerList = append(workerList, fmt.Sprintf("worker-%d", i))
	}
	oldAssignMap = gotToAssignMap
	gotToDeleteWorkerKey, gotToDeleteTaskKey, gotToAssignMap, err = a.GetReBalanceResult(workerList, taskMap, oldAssignMap)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ToDeleteWorkerKey:", gotToDeleteWorkerKey)
	t.Log("ToDeleteTaskKey:", gotToDeleteTaskKey)
	t.Log("ToAssignMap:", gotToAssignMap)
}
