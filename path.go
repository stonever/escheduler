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
func ParseTaskAbbrFromTaskKey(key string) (string, error) {
	arr := strings.SplitAfterN(key, "/", 5)
	if len(arr) < 5 {
		return "", errors.New("invalid task key :" + key)
	}
	return arr[4], nil
}
func ParseWorkerFromWorkerKey(key string) (string, error) {
	arr := strings.Split(key, "/")
	if len(arr) != 4 {
		return "", errors.New("invalid job :" + key)
	}
	return arr[3], nil
}

// ParseWorkerFromTaskKey /20220624/task/192.168.193.131-28682/raw data for task 10
// return 192.168.193.131-28682
func ParseWorkerFromTaskKey(key string) string {
	arr := strings.SplitN(key, "/", 5)
	if len(arr) >= 5 {
		return arr[3]
	}
	return ""
}
