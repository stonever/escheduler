package main

import "hash/fnv"

// hash return index is always less than num
func hash(key []byte, num int32) (int32, error) {
	hasher := fnv.New32a()
	_, err := hasher.Write(key)
	if err != nil {
		return -1, err
	}
	var partition = int32(hasher.Sum32()) % num
	if partition < 0 {
		partition = -partition
	}
	return partition, nil
}
