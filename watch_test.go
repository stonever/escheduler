package escheduler

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func TestNewWatcher(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	node := clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		Username:    "root",
		Password:    "password",
		DialTimeout: 5 * time.Second,
	}

	client, err := clientv3.New(node)
	if err != nil {
		t.Fatal(err)
	}
	got, err := NewWatcher(ctx, client, "/20220809")
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(time.Minute, func() {
		cancel()
	})
	for c := range got.EventChan {
		t.Log(c)
	}
	t.Log("success")
}
