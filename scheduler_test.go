package main

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"reflect"
	"testing"
)

func TestNewScheduler(t *testing.T) {
	type args struct {
		config SchedulerConfig
	}
	var arg = args{
		config: SchedulerConfig{
			EtcdConfig: clientv3.Config{
				Endpoints: []string{"127.0.0.1:2379"},
				Username:  "root",
				Password:  "password",
			},
			RootName: "go-mario",
		},
	}

	tests := []struct {
		name    string
		args    args
		want    Scheduler
		wantErr bool
	}{{
		name: "new scheduler",
		args: arg,
	}, {
		name: "new scheduler",
		args: arg,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewScheduler(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewScheduler() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			ctx := context.Background()
			go got.ElectLoop(ctx)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewScheduler() got = %v, want %v", got, tt.want)
			}
			select {}
		})
	}
}
