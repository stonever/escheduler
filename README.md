# escheduler
It is a distributed task scheduling framework that supports a variety of load balancing strategies, and the client handles task addition and deletion by listening to the task status.

# Principle
1. first start, worker go to barrier together, ensure scheduler do not schedule meaningless.
2. after first schedule worker watcher wait for a while and then re-balance
3. worker left barrier together, ensure external program can get when all tasks are running, for k8s rolling update
4. priority queue is for worker can start task according to their priority
5. balancer use least-load as assign algorithm, ensure there is no worker effort too much.
6. balancer reassignment uses a sticky strategy, change as little as possible.
7. check worker num < max num before register worker info in etcd by using mutex


