package escheduler

type RawData []byte
type Task struct {
	Key  string  // if not empty, will use hash-rebalance
	Abbr string  // a short name which uniquely identify the task, if empty, abbr will use
	Raw  RawData // task value, []byte
}
