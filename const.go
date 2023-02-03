package escheduler

const (
	electionFolder = "election"
	workerFolder   = "worker"
	taskFolder     = "task"
	workerBarrier  = "worker-barrier"
)
const (
	ReasonFirstSchedule = "first schedule"
)
const (
	WorkerStatusNew         = iota // 0
	WorkerStatusRegister           // 1
	WorkerStatusInBarrier          // 2
	WorkerStatusLeftBarrier        // 3
	WorkerStatusDead               // 4
)
const (
	WorkerValueRunning = "running"
)
