package escheduler

type RawData []byte
type Task struct {
	P     float64
	Key   string  // if not empty, will use hash-rebalance
	Group string  //
	ID    string  // a short name which uniquely identify the task
	Raw   RawData // task value, []byte
}
type WorkerTask struct {
	Task
	worker string
}
