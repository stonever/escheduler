package escheduler

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Node struct {
	EtcdConfig clientv3.Config
	RootName   string
	// TTL configures the session's TTL in seconds.
	// If TTL is <= 0, the default 60 seconds TTL will be used.
	TTL    int64 // worker registered in etcd
	client *clientv3.Client
	MaxNum int    // total worker num + 1 scheduler
	Name   string // if not set, default {ip}-{pid}
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
	if len(n.Name) == 0 {
		var err error
		n.Name, err = n.GetDefaultName()
		if err != nil {
			return errors.Wrapf(err, "cannot generate default name for node")
		}
	}
	return nil
}
func (n Node) GetDefaultName() (string, error) {
	ip, err := GetLocalIP()
	if err != nil {
		return "", errors.Wrapf(err, "cannot get local ip")
	}
	pid := os.Getpid()
	name := fmt.Sprintf("%s-%d", ip, pid)
	return name, nil
}
