package escheduler

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stonever/balancer/balancer"
	"github.com/stonever/escheduler/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/zap"
)

func TestStopScheduler(t *testing.T) {
	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName: "20220624",
		TTL:      15,
		MaxNum:   2,
	}
	schedConfig := MasterConfig{
		Interval:  time.Second * 10,
		Generator: tg,
		Timeout:   time.Minute,
	}

	sc, err := NewMaster(schedConfig, node)
	if err != nil {
		log.Fatal(err.Error())
	}
	go func() {
		time.Sleep(time.Second * 30)
		sc.Stop()
	}()
	sc.Start()

}
func TestMultiMaster(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			newMaster("root", "worker1", 1)
			wg.Done()
		}()
	}
	wg.Wait()
}
func newMaster(rootName string, name string, num int) (worker Worker, master Master) {
	maxNum := num + 1
	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:23790"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName: rootName,
		TTL:      15,
		MaxNum:   maxNum,
		Name:     name,
	}
	schedConfig := MasterConfig{
		Interval:      time.Second * 60,
		ReBalanceWait: 5 * time.Second,
		Generator:     tg,
	}
	go func() {
		var err error
		worker, err = NewWorker(node)
		if err != nil {
			log.Fatal(err.Error())
		}
		worker.Start()

		for v := range worker.WatchTask() {
			log.Info("receive", zap.Any("v", v))
		}
	}()
	var err error
	master, err = NewMaster(schedConfig, node)
	if err != nil {
		log.Fatal(err.Error())
	}
	go func() {
		master.Start()
	}()

	return worker, master
}

func tg(ctx context.Context) ([]Task, error) {
	ret := make([]Task, 0)
	i := 0
	for {
		task := Task{
			ID:  fmt.Sprintf("%d", i),
			Raw: []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
		}
		ret = append(ret, task)
		i++
		if i == 20 {
			break
		}
	}
	return ret, nil
}
func TestDoubleBarrie(t *testing.T) {
	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName: "20220624",
		TTL:      15,
	}
	client, err := clientv3.New(node.EtcdConfig)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {

		go func() {
			key := "/test_barrier"
			s, err := concurrency.NewSession(client)
			if err != nil {
				t.Error(err)
				return
			}
			b := recipe.NewDoubleBarrier(s, key, 1)
			err = b.Enter()
			if err != nil {
				log.Fatal(err.Error())
			}
			log.Info("enter...")
			err = s.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}()
	}
	select {}
}
func TestWatchTaskDel(t *testing.T) {

	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName: fmt.Sprintf("kline-pump-%d", time.Now().Unix()),
		TTL:      15,
		MaxNum:   2,
	}
	schedConfig := MasterConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			i := time.Now().Minute()
			if i%2 == 1 {
				task := Task{
					ID:  fmt.Sprintf("%d", i),
					Raw: []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
				}
				ret = append(ret, task)
			}
			return
		},
	}
	worker, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	go func() {
		worker.Start()
	}()
	go func() {
		for v := range worker.WatchTask() {
			log.Info("receive", zap.Any("v", v))
		}
	}()
	sc, err := NewMaster(schedConfig, node)
	if err != nil {
		log.Fatal(err.Error())
	}
	sc.Start()

	//select {}
}

// TestHashBalancer if all task has has key and are assigned to single worker,
// if rebalance , will stickey strategy be still effective
func TestHashBalancer(t *testing.T) {

	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName: "20230109",
		TTL:      15,
		MaxNum:   2,
	}
	schedConfig := MasterConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			for i := 0; i < 10; i++ {
				task := Task{
					ID:  fmt.Sprintf("%d", i),
					Raw: []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
					Key: "ig",
				}
				ret = append(ret, task)
			}
			ret = append(ret, Task{
				ID:  fmt.Sprintf("%d", 10),
				Raw: []byte(fmt.Sprintf("raw data without key")),
			})
			return
		},
	}
	go func() {
		node.Name = "aaa"
		worker, err := NewWorker(node)
		if err != nil {
			log.Fatal(err.Error())
		}
		go worker.Start()

		for v := range worker.WatchTask() {
			log.Info("receive", zap.Any("v", v))
		}
		err = worker.TryLeaveBarrier()
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
	go func() {
		time.Sleep(time.Second * 30)
		node.Name = "bbb"

		worker, err := NewWorker(node)
		if err != nil {
			log.Fatal(err.Error())
		}
		worker.Start()

		for v := range worker.WatchTask() {
			log.Info("receive", zap.Any("v", v))
		}
	}()
	sc, err := NewMaster(schedConfig, node)
	if err != nil {
		log.Fatal(err.Error())
	}
	sc.Start()

	//select {}
}

// TestWorkerRestart if all task has key and are assigned to single worker,
// if rebalance , will stickey strategy be still effective
func TestWorkerRestart(t *testing.T) {

	rootName := "escheduler" + strconv.Itoa(int(time.Now().Unix()))

	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName: rootName,
		TTL:      15,
		MaxNum:   3 + 1,
	}
	schedConfig := MasterConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			for i := 0; i < 5; i++ {
				task := Task{
					ID:  fmt.Sprintf("%d", i),
					Raw: []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
				}
				ret = append(ret, task)
			}
			return
		},
	}
	node.Name = "worker1"
	worker1, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	node.Name = "worker2"

	worker2, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	node.Name = "worker3"
	worker3, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}

	go func() {
		worker1.Start()

	}()
	go func() {
		worker2.Start()
	}()
	go func() {
		worker3.Start()
	}()
	go func() {
		sc, err := NewMaster(schedConfig, node)
		if err != nil {
			log.Fatal(err.Error())
		}
		sc.Start()
		log.Fatal("exit")

	}()
	eventC1 := worker1.WatchTask()
	eventC2 := worker2.WatchTask()
	eventC3 := worker3.WatchTask()

	go func() {
		for {
			select {
			case <-time.Tick(time.Second * 60):
				worker1.Stop()
				worker1.Start()
				eventC1 = worker1.WatchTask()
			case <-time.Tick(time.Second * 30):
				err = worker1.TryLeaveBarrier()
			case task := <-eventC1:
				t.Logf("worker1 do task:%s", task)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-time.Tick(time.Second * 30):
				err = worker1.TryLeaveBarrier()
			case task := <-eventC2:
				t.Logf("worker2 do task:%s", task)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-time.Tick(time.Second * 30):
				err = worker3.TryLeaveBarrier()
			case task := <-eventC3:
				t.Logf("worker3 do task:%s", task)
			}
		}
	}()
	select {}
}
func TestLoadBalancer(t *testing.T) {
	workerNum := 3
	node := Node{
		RootName: "20220809",
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		TTL:    60,
		MaxNum: workerNum + 1,
	}
	schedConfig := MasterConfig{
		Interval: time.Second * 10,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			for _, group := range []string{"A", "B", "C"} {
				for i := 0; i < 5; i++ {
					task := Task{
						ID:    fmt.Sprintf("%s%d", group, i),
						Raw:   []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
						Group: group,
						P:     float64(i),
					}
					ret = append(ret, task)
				}
			}

			return
		},
	}
	s, err := NewMaster(schedConfig, node)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		s.Start()
		log.Fatal("exit")

	}()
	for workerN := 0; workerN < workerNum; workerN++ {
		node.Name = fmt.Sprintf("worker-%d", workerN)
		worker1, err := NewWorker(node)
		if err != nil {
			log.Fatal(err.Error())
		}
		go func() {
			//time.AfterFunc(time.Minute*2, func() {
			//	worker1.Stop()
			//})
			worker1.Start()
		}()

	}
	select {}

}
func TestLeastLoad(t *testing.T) {
	workerList := []string{"worker-1", "worker-2", "worker-3"}
	leastLoadBalancer, _ := balancer.Build(balancer.LeastLoadBalancer, workerList)
	leastLoadBalancer.Inc("worker-1")
	res, err := leastLoadBalancer.Balance("")
	t.Logf(res, err)
}
func TestMainFunction(t *testing.T) {
	rootNme := time.Now().Format(time.RFC3339)
	num := 5
	for i := 0; i < num; i++ {
		name := fmt.Sprintf("worker-%d", i)
		go func() {
			newMaster(rootNme, name, num)
		}()
	}
	time.Sleep(time.Second)
	etcdConfig := clientv3.Config{
		Endpoints:   []string{"127.0.0.1:23790"},
		Username:    "root",
		Password:    "password",
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(etcdConfig)
	if err != nil {
		t.Fatal(err)
	}
	// 5 worker should be registered
	getResp, err := client.Get(context.Background(), fmt.Sprintf("/%s/worker/", rootNme), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	assert.EqualValues(t, 5, getResp.Count)
	time.Sleep(time.Second * 1)
	// 20 tasks should be assigned
	var taskMap = make(map[string]*clientv3.GetResponse)
	for i := 0; i < num; i++ {
		name := fmt.Sprintf("/%s/task/worker-%d", rootNme, i)
		getResp, err := client.Get(context.Background(), name, clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		assert.EqualValues(t, 4, getResp.Count)
		taskMap[name] = getResp
	}

	// delete worker-0
	worker0 := fmt.Sprintf("/%s/worker/%s", rootNme, "worker-0")
	delResp, err := client.Delete(context.Background(), worker0)
	if err != nil {
		t.Fatal(err)
	}
	assert.EqualValues(t, 1, delResp.Deleted)
	time.Sleep(time.Second * 1) // waiting register again
	// worker-0 should be registered again
	getResp, err = client.Get(context.Background(), worker0)
	if err != nil {
		t.Fatal(err)
	}
	assert.EqualValues(t, getResp.Count, 1)

	// task should not be changed
	var taskMap2 = make(map[string]*clientv3.GetResponse)
	for i := 0; i < num; i++ {
		name := fmt.Sprintf("/%s/task/worker-%d", rootNme, i)
		getResp, err := client.Get(context.Background(), name, clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		assert.EqualValues(t, 4, getResp.Count)
		taskMap2[name] = getResp
	}
	for k, v := range taskMap {
		assert.EqualValues(t, v.Count, taskMap2[k].Count)
		assert.EqualValues(t, len(v.Kvs), len(taskMap2[k].Kvs))
	}
	for k, v := range taskMap {
		v2 := taskMap2[k]
		for index, kv := range v.Kvs {
			assert.EqualValues(t, kv.Key, v2.Kvs[index].Key)
			assert.EqualValues(t, kv.Value, v2.Kvs[index].Value)
		}
	}

}
