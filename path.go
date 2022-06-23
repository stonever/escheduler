package main

import (
	"github.com/pkg/errors"
	"strings"
)

func ParseTaskFromKey(key string) (RawData, error) {
	arr := strings.SplitAfterN(key, "/", 3)
	if len(arr) < 3 {
		return nil, errors.New("invalid job :" + key)
	}
	return RawData(arr[2]), nil
}
func ParseWorkerFromKey(key string) (string, error) {
	arr := strings.Split(key, "/")
	if len(arr) != 4 {
		return "", errors.New("invalid job :" + key)
	}
	return arr[3], nil
}
