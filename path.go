package escheduler

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
)

func ParseTaskFromValue(value []byte) (task Task, err error) {
	err = json.Unmarshal(value, &task)
	return
}

// ParseTaskIDFromTaskKey
//
//	@Description: task id is at the end
//	@param rootName
//	@param key
//	@return string
//	@return error
func ParseTaskIDFromTaskKey(rootName, key string) (string, error) {
	// key is /root/task/192.168.193.131-125075/10
	// change to task/192.168.193.131-125075/10
	key = strings.Replace(key, "/"+rootName+"/", "", 1)
	expected := 3
	arr := strings.SplitAfterN(key, "/", expected)
	if len(arr) < expected { // should be 3
		return "", errors.New("invalid format task key:" + key)
	}
	return arr[len(arr)-1], nil
}

// ParseWorkerIDFromWorkerKey
//
//	@Description: worker id is at the end
//	@param rootName
//	@param key
//	@return string
//	@return error
func ParseWorkerIDFromWorkerKey(rootName, key string) (string, error) {
	// key is /root/worker/mq455
	// change to worker/mq455
	key = strings.Replace(key, "/"+rootName+"/", "", 1)
	expected := 2
	arr := strings.SplitAfterN(key, "/", expected)
	if len(arr) < expected {
		return "", errors.New("invalid format worker key:" + key)
	}
	return arr[len(arr)-1], nil
}

// ParseWorkerIDFromTaskKey
//
//	@Description:
//	@param rootName, like root
//	@param key like /root/task/192.168.193.131-28682/raw data for task 10
//	@return string 192.168.193.131-28682
func ParseWorkerIDFromTaskKey(rootName, key string) (string, error) {
	key = strings.Replace(key, "/"+rootName+"/", "", 1)
	expected := 3
	arr := strings.SplitN(key, "/", expected)
	if len(arr) < expected {
		return "", errors.New("invalid format task key:" + key)
	}
	return arr[1], nil
}
