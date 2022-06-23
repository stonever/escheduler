package main

type RawData []byte

type Task struct {
	Key string
	Raw RawData
}

// todo
type assignedTask struct {
	rev int64 // task assined by which revision leader, key is leader's key
	Task
}
