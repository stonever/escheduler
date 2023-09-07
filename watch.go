package escheduler

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Watcher struct {
	revision      int64                  // keep latest revision number
	EventChan     <-chan *clientv3.Event // output event channel
	eventChanSize int

	incipientRevision int64              // initial revision
	IncipientKVs      []*mvccpb.KeyValue // initial kv with prefix
	blocking          bool               // check if event channel blocking
	logger            *slog.Logger
}

// NewWatcher
// 关于 watch 哪个版本：
// watch 某一个 key 时，想要从历史记录开始就用 CreateRevision，最新一条(这一条直接返回) 开始就用 ModRevision 。
// watch 某个前缀，就必须使用 Revision。如果要watch当前前缀后续的变化，则应该从当前集群的 Revision+1 版本开始watch。
func NewWatcher(ctx context.Context, client *clientv3.Client, pathPrefix string) (*Watcher, error) {
	// 在watchChan产生之前，task发生了增删，也会被感知到，进行同步
	resp, err := client.KV.Get(ctx, pathPrefix, clientv3.WithPrefix())
	if err != nil {
		err = errors.Wrapf(err, "get kv with prefix error, path:%s", pathPrefix)
		return nil, err
	}
	eventChan := make(chan *clientv3.Event, 64)
	w := &Watcher{
		eventChanSize:     64,
		EventChan:         eventChan,
		incipientRevision: resp.Header.Revision,
		IncipientKVs:      resp.Kvs,
		logger:            slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("watcher", pathPrefix),
	}

	w.revision = resp.Header.Revision + 1

	go func() {
		for {
			select {
			case <-ctx.Done():
				w.logger.Info("ctx done. stop watcher", "prefix", pathPrefix, "revision", w.revision)
				close(eventChan)
				return
			default:

			}
			rch := client.Watch(ctx, pathPrefix, clientv3.WithPrefix(), clientv3.WithCreatedNotify(), clientv3.WithRev(w.revision))
			w.logger.Info("start watcher...", "prefix", pathPrefix, "revision", w.revision)
			//if ctx done, rch will be closed, for loop will end
			for n := range rch {
				if n.Created {
					w.logger.Info("watcher created")
				}
				// 一般情况下，协程的逻辑会阻塞在此
				if n.CompactRevision > w.revision {
					w.revision = n.CompactRevision
					w.logger.Info("set revision to CompactRevision", "new revision", w.revision)
				}
				// 是否需要更新当前的最新的 revision
				if n.Header.GetRevision() > w.revision {
					w.revision = n.Header.GetRevision()
				}
				if err := n.Err(); err != nil {
					w.logger.Error("watcher response error", "response", spew.Sdump(n))
					break
				}
				for _, ev := range n.Events {
					w.blocking = true
					eventChan <- ev // may be  blocked
					w.blocking = false
				}
			}
			time.Sleep(time.Second)
			w.logger.Info("watcher need to restart", "prefix", pathPrefix, "revision", w.revision)
		}
	}()

	return w, nil
}
