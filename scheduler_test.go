package escheduler

import (
	"context"
	"fmt"
	"github.com/stonever/escheduler/log"
	clientv3 "go.etcd.io/etcd/client/v3"
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
		c, err := worker.Start(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		for v := range c {
			log.Info("receive", zap.Any("value", v))
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
			Key: fmt.Sprintf("%d", i),
			Raw: []byte(fmt.Sprintf("raw data for task %d", i)),
		}
		ret = append(ret, task)
		i++
		if i == 20 {
			break
		}
	}
	return ret, nil
}
