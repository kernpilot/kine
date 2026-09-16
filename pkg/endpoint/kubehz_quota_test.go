package endpoint_test

// KUBEHZ-PATCH P2 test: the wiring in endpoint.Listen. A real kine on sqlite
// (cgo and nocgo builds both have a driver) with QuotaBytes set refuses a
// client Put with etcd's no-space error and still serves reads and deletes;
// without QuotaBytes the same Put succeeds. Deleting the P2 block in Listen
// turns the first case green for the wrong reason, so this test fails.

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/sqlite"
	"github.com/k3s-io/kine/pkg/endpoint"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
)

func TestKubehzP2Listen(t *testing.T) {
	for _, tc := range []struct {
		name  string
		quota int64
		want  error // what the etcd client hands back; it maps gRPC status to rpctypes
	}{
		{"one byte limit refuses the put", 1, rpctypes.ErrNoSpace},
		{"no limit accepts the put", 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := listenSQLite(t, tc.quota)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err := client.Put(ctx, "/a", "v")
			if !errors.Is(err, tc.want) {
				t.Fatalf("Put with QuotaBytes=%d: got %v, want %v", tc.quota, err, tc.want)
			}
			if tc.want == nil {
				return
			}
			// the same code and text etcd sends
			var etcdErr rpctypes.EtcdError
			if !errors.As(err, &etcdErr) || etcdErr.Code() != codes.ResourceExhausted ||
				etcdErr.Error() != "etcdserver: mvcc: database space exceeded" {
				t.Fatalf("Put error on the wire: %#v", err)
			}

			// reads and deletes still work above the limit
			if _, err := client.Get(ctx, "/a"); err != nil {
				t.Fatalf("Get above the limit: %v", err)
			}
			del, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("/a"), "=", 0)).
				Then(clientv3.OpDelete("/a")).
				Else(clientv3.OpGet("/a")).
				Commit()
			if err != nil {
				t.Fatalf("delete Txn above the limit: %v", err)
			}
			if !del.Succeeded {
				t.Fatal("delete Txn did not succeed on an absent key")
			}
		})
	}
}

// listenSQLite starts kine on a fresh sqlite database behind a unix socket
// and returns a connected client. Everything stops with the test.
func listenSQLite(t *testing.T, quota int64) *clientv3.Client {
	t.Helper()
	// a short path: unix socket paths are limited to about 100 bytes
	dir, err := os.MkdirTemp("", "kine-p2-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	e, err := endpoint.Listen(ctx, endpoint.Config{
		WaitGroup:      wg,
		Listener:       "unix://" + dir + "/kine.sock",
		Endpoint:       "sqlite://" + dir + "/state.db?" + sqlite.DefaultParams,
		NotifyInterval: 5 * time.Second,
		// the flag defaults; the backend refuses zero values for these
		CompactTimeout:   5 * time.Second,
		CompactMinRetain: 1000,
		CompactBatchSize: 1000,
		PollBatchSize:    500,
		QuotaBytes:       quota,
	})
	if err != nil {
		t.Fatal(err)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}
