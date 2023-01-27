package escheduler

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
	"testing"
)

func TestAssigner_GetReBalanceResult(t *testing.T) {
	var a Assigner
	workerList := make([]string, 0)
	workerList = append(workerList, "worker-a", "worker-b", "worker-c")
	taskPathResp := make([]*mvccpb.KeyValue, 0)
	taskMap := make(map[string]Task)
	gotToDeleteWorkerTaskKey, gotToDeleteTaskKey, gotAssignMap, err := a.GetReBalanceResult(workerList, taskMap, taskPathResp)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(gotToDeleteWorkerTaskKey)
	t.Log(gotToDeleteTaskKey)
	t.Log(gotAssignMap)

}
