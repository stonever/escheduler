package main

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schedConfig := SchedulerConfig{
		RootName: "20220623",
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		JobConfig: JobConfig{
			Interval: time.Second * 5,
		},
		NodeTTL: 60,
	}
	go func() {
		workerConfig := WorkerConfig{
			RootPath: "20220623",
			EtcdConfig: clientv3.Config{
				Endpoints:   []string{"127.0.0.1:2379"},
				Username:    "root",
				Password:    "password",
				DialTimeout: 5 * time.Second,
			},
			NodeTTL: 60,
		}
		worker, err := NewWorker(workerConfig)
		if err != nil {
			log.Fatal(err)
		}
		c, err := worker.Start(ctx)
		if err != nil {
			log.Fatal(err)
		}
		for v := range c {
			log.Printf("receive %s", v)
		}
	}()
	sched, err := NewScheduler(schedConfig, tg)
	if err != nil {
		log.Fatal(err)
	}
	err = sched.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func tg(ctx context.Context) ([]Task, error) {
	ret := make([]Task, 0)
	i := 0
	for {
		task := Task{
			Key: fmt.Sprintf("%d", i),
			Raw: []byte(fmt.Sprintf("raw data for task %d", i)),
		}
		ret = append(ret, task)
		i++
		if i == 100 {
			break
		}
	}
	return ret, nil
}
