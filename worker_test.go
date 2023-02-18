package escheduler

import (
	"context"
	"fmt"
	"github.com/sourcegraph/conc"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stonever/escheduler/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
)

func TestBarrier_AllLeftNewEnter(t *testing.T) {
	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName: "20220704-barrier",
		TTL:      15,
		MaxNum:   3,
	}
	client, err := clientv3.New(node.EtcdConfig)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		s1, err := concurrency.NewSession(client)
		if err != nil {
			t.Error(err)
			return
		}
		b1 := recipe.NewDoubleBarrier(s1, GetWorkerBarrierName(node.RootName), node.MaxNum)
		t.Log("s1 b1 try to enter")
		err = b1.Enter()
		if err != nil {
			t.Error(err)
			return
		}

		t.Log("b1 try to leave")

		err = b1.Leave()
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("s1 b left")

	}()
	go func() {
		s2, err := concurrency.NewSession(client)
		if err != nil {
			t.Error(err)
			return
		}
		b1 := recipe.NewDoubleBarrier(s2, GetWorkerBarrierName(node.RootName), node.MaxNum)
		t.Log("s2 b1 try to enter")
		err = b1.Enter()
		if err != nil {
			t.Error(err)
			return
		}

		t.Log("s2 b1 try to leave")

		err = b1.Leave()
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("s2 b1 left")

	}()
	time.Sleep(time.Second * 5)
	s2, err := concurrency.NewSession(client)
	if err != nil {
		t.Fatal(err)
	}
	b2 := recipe.NewDoubleBarrier(s2, GetWorkerBarrierName(node.RootName), node.MaxNum)
	t.Log("b2 try to enter")

	err = b2.Enter()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("b2 try to leave")
	err = b2.Leave()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("all left")

	lastS, err := concurrency.NewSession(client)
	if err != nil {
		t.Fatal(err)
	}
	lastWorker := recipe.NewDoubleBarrier(lastS, GetWorkerBarrierName(node.RootName), 0)
	t.Log("lastWorker try to enter")

	err = lastWorker.Enter()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("lastWorker enter")
	err = lastWorker.Leave()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("lastWorker left")
}
func TestWorkerStatus(t *testing.T) {
	rootName := "escheduler" + strconv.Itoa(int(time.Now().Unix()))

	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:49720"},
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
			for i := 0; i < 3; i++ {
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
	var wg conc.WaitGroup
	wg.Go(func() {
		worker1.Start()
	})

	wg.Go(func() {
		worker2.Start()
	})

	wg.Go(func() {
		sc, err := NewMaster(schedConfig, node)
		if err != nil {
			log.Fatal(err.Error())
		}
		sc.Start()
	})

	assert.Equal(t, WorkerStatusNew, worker1.Status())
	time.Sleep(time.Second)
	assert.Equal(t, WorkerStatusRegister, worker1.Status())

	wg.Go(func() {
		worker3.Start()
		assert.Equal(t, worker1.Status(), WorkerStatusInBarrier)
	})
	time.Sleep(time.Second)
	assert.Equal(t, worker1.Status(), WorkerStatusInBarrier)
	assert.Equal(t, WorkerStatusInBarrier, worker1.Status())
	wg.Wait()
	//go func() {
	//	task := <-eventC1
	//	Convey("worker1 received one task", t, func() {
	//		So(task, ShouldNotBeEmpty)
	//		So(len(eventC1), ShouldEqual, 0)
	//	})
	//}()
	//
	//err = worker1.TryLeaveBarrier()
	//Convey("worker status switch to running ", t, func() {
	//	So(err, ShouldBeNil)
	//	So(worker1.Status(), ShouldEqual, WorkerStatusInBarrier)
	//})
	//select {
	//case status = <-c:
	//default:
	//	Convey("worker status did not switch ", t, func() {
	//
	//	})
	//}
	//go func() {
	//	task := <-eventC2
	//	Convey("worker2 received one task", t, func() {
	//		So(task, ShouldNotBeEmpty)
	//		So(len(eventC1), ShouldEqual, 0)
	//	})
	//}()
	//time.Sleep(time.Second)
	//err = worker1.TryLeaveBarrier()
	//Convey("worker status switch to running ", t, func() {
	//	So(err, ShouldBeNil)
	//})
	//
	//err = worker2.TryLeaveBarrier()
	//Convey("worker status switch to running ", t, func() {
	//	So(err, ShouldBeNil)
	//})
	//err = worker3.TryLeaveBarrier()
	//Convey("worker status switch to running ", t, func() {
	//	So(err, ShouldBeNil)
	//})
	//status = <-c
	//Convey("worker status be LeftBarrier", t, func() {
	//	So(status, ShouldEqual, WorkerStatusLeftBarrier)
	//})
	//
	//worker1.Stop()
	//status = <-c
	//Convey("worker status switch to dead ", t, func() {
	//	So(status, ShouldEqual, WorkerStatusDead)
	//})

}

// TestWorkerTooMuch worker max num = 3, but started 5 worker
// last 2 worker should be intercepted
func TestWorkerTooMuch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rootName := "escheduler/" + strconv.Itoa(int(time.Now().Unix()))
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
	var (
		workers []Worker
	)
	for i := 0; i < 5; i++ {
		node.Name = "worker" + strconv.Itoa(i)
		worker := startWorker(ctx, node)
		workers = append(workers, worker)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Second * 5)
	Convey("worker1 register", t, func() {
		So(workers[0].Status(), ShouldEqual, WorkerStatusRegister)
	})
	Convey("worker2 received one task", t, func() {
		So(workers[1].Status(), ShouldEqual, WorkerStatusRegister)
	})
	Convey("worker3 received one task", t, func() {
		So(workers[2].Status(), ShouldEqual, WorkerStatusRegister)
	})
	Convey("worker3 received one task", t, func() {
		So(workers[3].Status(), ShouldEqual, WorkerStatusNew)
	})
	Convey("worker3 received one task", t, func() {
		So(workers[4].Status(), ShouldEqual, WorkerStatusNew)
	})
}
func startWorker(ctx context.Context, node Node) Worker {
	worker, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	go func() {
		worker.Start()
	}()
	return worker
}

// TestWorkerStatusDead if worker start return, worker status should be dead
func TestWorkerStatusDead(t *testing.T) {
	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName: "escheduler/" + strconv.Itoa(int(time.Now().Unix())),
		TTL:      15,
		MaxNum:   3 + 1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	node.Name = "worker1"
	worker1 := startWorker(ctx, node)
	select {
	case <-ctx.Done():
		time.Sleep(time.Second)
		Convey("worker1 received one task", t, func() {
			status := worker1.Status()
			So(status, ShouldEqual, WorkerStatusDead)
		})
	}
}
func TestWorkerGetAllTask(t *testing.T) {
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
		MaxNum:   2 + 1,
	}
	schedConfig := MasterConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			for i := 0; i < 3; i++ {
				task := Task{
					ID:  fmt.Sprintf("ID-%d", i),
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

	go func() {
		worker1.Start()
	}()
	go func() {
		worker2.Start()
	}()

	go func() {
		sc, err := NewMaster(schedConfig, node)
		if err != nil {
			log.Fatal(err.Error())
		}
		sc.Start()

	}()

	for {
		task1, err1 := worker1.Tasks(ctx)
		task2, err2 := worker2.Tasks(ctx)

		Convey("err!=nil", t, func() {
			So(err1, ShouldBeNil)
			So(err2, ShouldBeNil)
		})
		Convey("task len", t, func() {
			So(len(task1), ShouldBeGreaterThanOrEqualTo, 1)
			So(len(task2), ShouldBeGreaterThanOrEqualTo, 1)
		})
		time.Sleep(time.Second)
	}

}
func TestWorkerRegister(t *testing.T) {

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
		MaxNum:   2,
	}
	node.Name = "worker1"
	worker1, err := NewWorker(node)
	if err != nil {
		log.Fatal(err.Error())
	}
	schedConfig := MasterConfig{
		Interval: time.Minute,
		Generator: func(ctx context.Context) (ret []Task, err error) {
			for i := 0; i < 3; i++ {
				task := Task{
					ID:  fmt.Sprintf("%d", i),
					Raw: []byte(fmt.Sprintf("raw data for task %d %d", i, time.Now().UnixMilli())),
				}
				ret = append(ret, task)
			}
			return
		},
	}

	sc, err := NewMaster(schedConfig, node)
	if err != nil {
		log.Fatal(err.Error())
	}
	go sc.Start()
	worker1.Start()
}
func TestPanic(t *testing.T) {

	panic("aaa")
}
