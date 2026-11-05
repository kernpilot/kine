package endpoint

// KUBEHZ-PATCH P2 test: the wiring in Listen. A real kine on sqlite (cgo and
// nocgo builds both have a driver) with QuotaBytes set refuses a client Put
// with etcd's no-space error and still serves reads and deletes; without
// QuotaBytes the same Put succeeds; a live source that fails on the first
// sample fails Listen. Deleting the P2 block in Listen turns the first case
// green for the wrong reason, so this test fails.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/sqlite"
	"github.com/k3s-io/kine/pkg/server"
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
			e, err := listenSQLite(t, tc.quota)
			if err != nil {
				t.Fatal(err)
			}
			client, err := clientv3.New(clientv3.Config{Endpoints: e.Endpoints, DialTimeout: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err = client.Put(ctx, "/a", "v")
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

// TestKubehzP2ListenFailsOnFirstSample: a live source that fails at boot
// must fail Listen, not serve with no limit until the next tick.
func TestKubehzP2ListenFailsOnFirstSample(t *testing.T) {
	seam := liveSizeOf
	t.Cleanup(func() { liveSizeOf = seam })
	liveSizeOf = func(server.Backend) server.SizeSource {
		return func(context.Context) (int64, error) { return 0, errors.New("relation kine does not exist") }
	}
	_, err := listenSQLite(t, 1)
	if err == nil || !strings.Contains(err.Error(), "quota: the first size sample failed") ||
		!strings.Contains(err.Error(), "relation kine does not exist") {
		t.Fatalf("Listen with a failing live source: got %v, want the first-sample error", err)
	}
}

// TestKubehzP2ListenRelief: B481 end to end on a real kine (sqlite). Fill the
// database to its limit. A new key is then refused, while an update of an
// existing Lease and an update that does not grow a value still pass. Delete
// the filled keys: the quota's relief compacts the history by itself and a
// new key is accepted again, with no other step. Without the relief (upstream
// compactor off, as in the KKP render) the deleted rows stay and new keys stay
// refused, which is the lock B481 found.
func TestKubehzP2ListenRelief(t *testing.T) {
	seam := quotaSampleInterval
	t.Cleanup(func() { quotaSampleInterval = seam })
	quotaSampleInterval = 100 * time.Millisecond

	e, err := listenSQLite(t, 512<<10)
	if err != nil {
		t.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: e.Endpoints, DialTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const lease = "/registry/leases/kube-system/k3s"
	if _, err := client.Put(ctx, lease, strings.Repeat("l", 200)); err != nil {
		t.Fatal(err)
	}
	blob := strings.Repeat("0123456789abcdef", 1024) // 16 KiB
	var filled []string
	for i := 0; ; i++ {
		key := fmt.Sprintf("/registry/configmaps/fill/%03d", i)
		_, err := client.Put(ctx, key, blob)
		if errors.Is(err, rpctypes.ErrNoSpace) {
			break
		}
		if err != nil {
			t.Fatalf("fill %s: %v", key, err)
		}
		if i > 200 {
			t.Fatal("200 puts of 16 KiB did not reach a 512 KiB limit")
		}
		filled = append(filled, key)
	}
	if len(filled) == 0 {
		t.Fatal("the first put was refused")
	}

	// full: a Lease renewal and a no-growth update pass, a new key does not
	if err := updateTxn(ctx, client, lease, strings.Repeat("r", 200)); err != nil {
		t.Fatalf("Lease update while full: %v", err)
	}
	if err := updateTxn(ctx, client, filled[0], blob[:len(blob)-100]); err != nil {
		t.Fatalf("no-growth update while full: %v", err)
	}
	if _, err := client.Put(ctx, "/registry/configmaps/default/new", "v"); !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("new key while full: got %v, want %v", err, rpctypes.ErrNoSpace)
	}

	for _, key := range filled {
		resp, err := client.Get(ctx, key)
		if err != nil || len(resp.Kvs) != 1 {
			t.Fatalf("get %s before the delete: %v", key, err)
		}
		del, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)).
			Then(clientv3.OpDelete(key)).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil || !del.Succeeded {
			t.Fatalf("delete %s while full: %v", key, err)
		}
	}

	// the relief needs two ticks: one to record the revision, one to compact
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err := client.Put(ctx, "/registry/configmaps/default/after", "v")
		if err == nil {
			return
		}
		if !errors.Is(err, rpctypes.ErrNoSpace) {
			t.Fatalf("put after the delete: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("new keys still refused 15 s after deleting every filled key: the relief did not free the space")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// updateTxn is the apiserver's update: compare the current mod revision, put.
func updateTxn(ctx context.Context, client *clientv3.Client, key, value string) error {
	resp, err := client.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) != 1 {
		return fmt.Errorf("%s not found", key)
	}
	txn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(key, value)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return err
	}
	if !txn.Succeeded {
		return fmt.Errorf("update of %s lost its compare", key)
	}
	return nil
}

// listenSQLite starts kine on a fresh sqlite database behind a unix socket.
// Everything stops with the test.
func listenSQLite(t *testing.T, quota int64) (ETCDConfig, error) {
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

	return Listen(ctx, Config{
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
}
