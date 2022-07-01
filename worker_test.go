package escheduler

import (
	"context"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stonever/escheduler/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func TestWorkerStatus(t *testing.T) {
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
		MaxNum:   3,
	}
	schedConfig := SchedulerConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			for i := 0; i < 3; i++ {
				task := Task{
					Abbr: fmt.Sprintf("%d", i),
					Raw:  []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
				}
				ret = append(ret, task)
			}
			return
		},
	}
	worker1, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	worker2, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	worker3, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	eventC1 := worker1.WatchTask()
	eventC2 := worker2.WatchTask()
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

	c := make(chan string)
	worker1.WatchStatus(c)
	status := <-c
	Convey("worker status switch to new ", t, func() {
		So(status, ShouldEqual, WorkerStatusNew)
	})
	status = <-c
	Convey("switch to WorkerStatusInBarrier ", t, func() {
		So(status, ShouldEqual, WorkerStatusInBarrier)
	})
	go func() {
		task := <-eventC1
		Convey("worker1 received one task", t, func() {
			So(task, ShouldNotBeEmpty)
			So(len(eventC1), ShouldEqual, 0)
		})
	}()

	err = worker1.TryLeaveBarrier()
	Convey("worker status switch to running ", t, func() {
		So(err, ShouldBeNil)
		So(worker1.Status(), ShouldEqual, WorkerStatusInBarrier)
	})
	select {
	case status = <-c:
	default:
		Convey("worker status did not switch ", t, func() {

		})
	}
	go func() {
		task := <-eventC2
		Convey("worker2 received one task", t, func() {
			So(task, ShouldNotBeEmpty)
			So(len(eventC1), ShouldEqual, 0)
		})
	}()

	err = worker2.TryLeaveBarrier()
	Convey("worker status switch to running ", t, func() {
		So(err, ShouldBeNil)
	})

	err = worker2.TryLeaveBarrier()
	Convey("worker status switch to running ", t, func() {
		So(err, ShouldBeNil)
	})
	err = worker3.TryLeaveBarrier()
	Convey("worker status switch to running ", t, func() {
		So(err, ShouldBeNil)
	})
	status = <-c
	Convey("worker status did not switch ", t, func() {
		So(status, ShouldEqual, WorkerStatusLeftBarrier)
	})

	worker1.Stop()
	status = <-c
	Convey("worker status switch to dead ", t, func() {
		So(status, ShouldEqual, WorkerStatusDead)
	})

}
