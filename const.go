package escheduler

const (
	electionFolder   = "election"
	workerFolder     = "worker"
	taskFolder       = "task"
	workerBarrier    = "worker_barrier"
	schedulerBarrier = "scheduler_barrier"
)
const (
	ReasonFirstSchedule = "first schedule"
)
const (
	WorkerStatusNew         = "new"
	WorkerStatusInBarrier   = "in_barrier"
	WorkerStatusLeftBarrier = "left_barrier"
	WorkerStatusDead        = "dead"
)
