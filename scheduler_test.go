package escheduler

import (
	"context"
	"fmt"
	"github.com/stonever/escheduler/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/zap"
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
		workerCfg := WorkerConfig{}
		c, err := worker.Start(ctx, workerCfg)
		if err != nil {
			log.Fatal(err.Error())
		}
		for v := range c {
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
		RootName: "20220624",
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
	go func() {
		worker, err := NewWorker(node)
		if err != nil {
			log.Fatal(err.Error())
		}
		workerCfg := WorkerConfig{}
		c, err := worker.Start(ctx, workerCfg)
		if err != nil {
			log.Fatal(err.Error())
		}
		for v := range c {
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
	select {}
}
