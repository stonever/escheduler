# escheduler

# todo

1. generate unique process id in distributed systems.
2. least load need incr

# point
1. worker goto barrier together, ensure scheduler do not schedule meaningless.
2. worker watcher wait for a while and then re-balance
3. worker left barrier together, ensure external program can get when all tasks are running, for k8s rolling update
4. priority queue is for worker can start task according to their priority(todo)
5. balancer use least-load as assign algorithm, ensure there is no worker effort too much.
6. balancer reassignment uses a sticky strategy, change as little as possible.



