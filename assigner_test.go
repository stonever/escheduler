package escheduler

import (
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestAssigner_GetReBalanceResult(t *testing.T) {
	var (
		a        = NewAssigner()
		rootName = "root1"
	)
	a.rootName = rootName
	workerList := make([]string, 0)
	workerList = append(workerList, "worker-a", "worker-b", "worker-c")
	taskPathResp := make([]*mvccpb.KeyValue, 0)
	taskMap := make(map[string]Task)
	oldAssignMap := make(map[string][]string)
	//ps := &parser{rootName: rootName}
	for _, kv := range taskPathResp {
		//worker := parseWorkerIDFromWorkerKey()
		oldAssignMap[string(kv.Key)] = append(oldAssignMap[string(kv.Key)], "")
	}
	gotToDeleteWorkerTaskKey, gotToDeleteTaskKey, gotAssignMap, err := a.GetReBalanceResult(workerList, taskMap, oldAssignMap)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(gotToDeleteWorkerTaskKey)
	t.Log(gotToDeleteTaskKey)
	t.Log(gotAssignMap)

}
