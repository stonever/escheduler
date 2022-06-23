package main

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type Node struct {
	etcdClient *clientv3.Client
}
type ScheduleCfg struct {
	ReBalanceWait time.Duration
}

func NewNode(ctx context.Context, config clientv3.Config) (*Node, error) {
	var (
		err error
		ret Node
	)

	// 建立连接
	ret.etcdClient, err = clientv3.New(config)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}
