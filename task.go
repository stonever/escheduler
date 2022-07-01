package escheduler

type RawData []byte
type Task struct {
	Pr   uint16
	Key  string  //
	Abbr string  // a short form of the task, if empty, abbr will use
	Raw  RawData // task value, []byte
}
