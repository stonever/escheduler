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
	WorkerStatusNew         = iota // new
	WorkerStatusRegister           // = "register"
	WorkerStatusInBarrier          // = "in_barrier"
	WorkerStatusLeftBarrier        // = "left_barrier"
	WorkerStatusDead               //  = "dead"
)
