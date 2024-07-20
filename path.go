package escheduler

import (
	"encoding/json"
	"path"
	"strings"

	"github.com/pkg/errors"
)

func NewPathParser(rootName string) *pathParser {
	return &pathParser{rootName: rootName}
}

type pathParser struct {
	rootName string
}

func (p pathParser) EncodeTaskFolderKey(worker string) string {
	taskFolderKey := path.Join("/"+p.rootName, taskFolder, worker)
	return taskFolderKey
}
func (p pathParser) EncodeTaskKey(worker, taskID string) string {
	taskPath := path.Join("/"+p.rootName, taskFolder)
	return path.Join(taskPath, worker, taskID)
}

func (p pathParser) parseTaskFromValue(value []byte) (task Task, err error) {
	err = json.Unmarshal(value, &task)
	return
}

// parseTaskIDFromTaskKey
//
//	@Description: task id is at the end
//	@param rootName
//	@param key
//	@return string
//	@return error
func (p pathParser) parseTaskIDFromTaskKey(key string) (string, error) {
	// key is /root/task/192.168.193.131-125075/10
	// change to task/192.168.193.131-125075/10
	expected := 3
	arr := strings.SplitAfterN(strings.Replace(key, "/"+p.rootName+"/", "", 1), "/", expected)
	if len(arr) < expected { // should be 3
		return "", errors.New("failed to parse taskID from key:" + key)
	}
	return arr[len(arr)-1], nil
}

// parseWorkerIDFromWorkerKey
//
//	@Description: worker id is at the end
//	@param rootName
//	@param key
//	@return string
//	@return error
func (p pathParser) parseWorkerIDFromWorkerKey(key string) (string, error) {
	rootName := p.rootName
	// key is /root/worker/mq455
	// change to worker/mq455
	expected := 2
	arr := strings.SplitAfterN(strings.Replace(key, "/"+rootName+"/", "", 1), "/", expected)
	if len(arr) < expected {
		return "", errors.New("invalid format worker key:" + key)
	}
	return arr[len(arr)-1], nil
}

// parseWorkerIDFromTaskKey
//
//	@Description:
//	@param rootName, like root
//	@param key like /root/task/192.168.193.131-28682/raw data for task 10
//	@return string 192.168.193.131-28682
func (p pathParser) parseWorkerIDFromTaskKey(key string) (string, error) {
	rootName := p.rootName

	key = strings.Replace(key, "/"+rootName+"/", "", 1)
	expected := 3
	arr := strings.SplitN(key, "/", expected)
	if len(arr) < expected {
		return "", errors.New("failed to parse workerID from key:" + key)
	}
	return arr[1], nil
}
