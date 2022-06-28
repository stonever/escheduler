package escheduler

import (
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Node struct {
	EtcdConfig clientv3.Config
	RootName   string
	// TTL configures the session's TTL in seconds.
	// If TTL is <= 0, the default 60 seconds TTL will be used.
	TTL    int64
	client *clientv3.Client
	MaxNum int
}

func (n *Node) Validation() error {
	if n.TTL == 0 {
		n.TTL = 10
	}
	if len(n.RootName) == 0 {
		return errors.New("RootName is required")
	}
	if n.MaxNum < 2 {
		return errors.Errorf("node maximum is %d <2 ", n.MaxNum)
	}
	return nil
}
