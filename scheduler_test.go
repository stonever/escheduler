package escheduler

import (
	"context"
	"fmt"
	"github.com/stonever/escheduler/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/zap"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestMultiScheuler(t *testing.T) {
	var wg sync.WaitGroup
	var (
		ctx, cancel = context.WithTimeout(context.Background(), time.Minute*5)
	)
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			newScheduler(ctx)
			wg.Done()
		}()
	}
	wg.Wait()
	cancel()
}
func newScheduler(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
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
	schedConfig := SchedulerConfig{
		Interval:  time.Second * 60,
		Generator: tg,
	}
	go func() {
		worker, err := NewWorker(node)
		if err != nil {
			log.Fatal(err.Error())
		}
		err = worker.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		for v := range worker.WatchTask() {
			log.Info("receive", zap.Any("v", v))
		}
	}()
	sc, err := NewScheduler(schedConfig, node)
	if err != nil {
		log.Fatal(err.Error())
	}
	err = sc.Start(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func tg(ctx context.Context) ([]Task, error) {
	ret := make([]Task, 0)
	i := 0
	for {
		task := Task{
			Abbr: fmt.Sprintf("%d", i),
			Raw:  []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
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
				t.Fatal(err)
			}
			b := recipe.NewDoubleBarrier(s, key, 1)
			b.Enter()
			log.Info("enter...")
			s.Close()
		}()
	}
	select {}
}
func TestWatchTaskDel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	schedConfig := SchedulerConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			i := time.Now().Minute()
			if i%2 == 1 {
				task := Task{
					Abbr: fmt.Sprintf("%d", i),
					Raw:  []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
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
		err = worker.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
	go func() {
		for v := range worker.WatchTask() {
			log.Info("receive", zap.Any("v", v))
		}
	}()
	sc, err := NewScheduler(schedConfig, node)
	if err != nil {
		log.Fatal(err.Error())
	}
	err = sc.Start(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	//select {}
}

// TestHashBalancer if all task has has key and are assigned to single worker,
// if rebalance , will stickey strategy be still effective
func TestHashBalancer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	schedConfig := SchedulerConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			for i := 0; i < 10; i++ {
				task := Task{
					Abbr: fmt.Sprintf("%d", i),
					Raw:  []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
					Key:  "ig",
				}
				ret = append(ret, task)
			}
			ret = append(ret, Task{
				Abbr: fmt.Sprintf("%d", 10),
				Raw:  []byte(fmt.Sprintf("raw data without key")),
			})
			return
		},
	}
	go func() {
		node.CustomName = "aaa"
		worker, err := NewWorker(node)
		if err != nil {
			log.Fatal(err.Error())
		}
		err = worker.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
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
		node.CustomName = "bbb"

		worker, err := NewWorker(node)
		if err != nil {
			log.Fatal(err.Error())
		}
		err = worker.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		for v := range worker.WatchTask() {
			log.Info("receive", zap.Any("v", v))
		}
	}()
	sc, err := NewScheduler(schedConfig, node)
	if err != nil {
		log.Fatal(err.Error())
	}
	err = sc.Start(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	//select {}
}

// TestWorkerRestart if all task has key and are assigned to single worker,
// if rebalance , will stickey strategy be still effective
func TestWorkerRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	schedConfig := SchedulerConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			for i := 0; i < 5; i++ {
				task := Task{
					Abbr: fmt.Sprintf("%d", i),
					Raw:  []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
				}
				ret = append(ret, task)
			}
			return
		},
	}
	node.CustomName = "worker1"
	worker1, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	node.CustomName = "worker2"

	worker2, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	node.CustomName = "worker3"
	worker3, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}

	go func() {
		err = worker1.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}

	}()
	go func() {
		err = worker2.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
	go func() {
		err = worker3.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
	go func() {
		sc, err := NewScheduler(schedConfig, node)
		if err != nil {
			log.Fatal(err.Error())
		}
		err = sc.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
	eventC1 := worker1.WatchTask()
	eventC2 := worker2.WatchTask()
	eventC3 := worker3.WatchTask()

	go func() {
		for {
			select {
			case <-time.Tick(time.Second * 60):
				worker1.Stop()
				worker1.Start(ctx)
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
