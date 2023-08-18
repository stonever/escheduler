package escheduler

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"go.uber.org/zap"
	"golang.org/x/exp/slog"
)

func TestQueueSamePriorityFIFO(t *testing.T) {

	node := Node{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			Username:    "root",
			Password:    "password",
			DialTimeout: 5 * time.Second,
		},
		RootName:    "20220624",
		TTL:         15,
		MaxNumNodes: 2,
	}
	client, err := clientv3.New(node.EtcdConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	q := recipe.NewPriorityQueue(client, node.RootName)
	go func() {

		err := q.Enqueue("coinbase", 0)
		if err != nil {
			t.Error(err)
			return
		}
		err = q.Enqueue("binance", 0)
		if err != nil {
			t.Error(err)
			return
		}
		err = q.Enqueue("huobi", 0)
		if err != nil {
			t.Error(err)
			return
		}
		err = q.Enqueue("okexv5", 0)
		if err != nil {
			t.Error(err)
			return
		}
	}()
	i := 0
	for {
		res, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
			return
		}
		slog.Info("接收值:", zap.Any("received", res))

		if i == 0 {
			Convey("first must be binance ", t, func() {
				So(res, ShouldEqual, "binance")
			})
		}
		if i == 1 {
			Convey("second must be huobi ", t, func() {
				So(res, ShouldEqual, "huobi")
			})
		}
		if i == 2 {
			Convey("third must be okexv5 ", t, func() {
				So(res, ShouldEqual, "okexv5")
			})
		}
		if i == 3 {
			Convey("last must be coinbase ", t, func() {
				So(res, ShouldEqual, "coinbase")
			})
			break
		}
		i++
	}
}
