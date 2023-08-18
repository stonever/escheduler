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
	WorkerStatusNew         = "new"          // 0
	WorkerStatusRegistered  = "registered"   // 1
	WorkerStatusInBarrier   = "in_barrier"   // 2
	WorkerStatusLeftBarrier = "left_barrier" // 3
	WorkerStatusDead        = "dead"         // 4
)
const (
	WorkerValueRunning = "running"
)
