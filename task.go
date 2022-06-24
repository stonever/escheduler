package escheduler

type RawData []byte

type Task struct {
	Key string
	Raw RawData
}
