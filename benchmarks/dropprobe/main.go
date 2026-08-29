// dropprobe — deliberately trigger kine's slow-consumer drop and record what
// the client actually observes.
//
// WHY THIS EXISTS. kine drops and unsubscribes a watcher whose buffer fills
// (broadcaster.go). The claim that the client is "told nothing" was inferred
// from reading that file alone, and it is wrong: server/watch.go does send a
// cancel when the events channel closes. The open question — and the only one
// that decides whether a patch is warranted — is what that cancel CARRIES:
//
//   - Canceled with CompactRevision != 0  -> the client knows its position is
//     invalid and relists. Correct behaviour, nothing to fix.
//   - Canceled with CompactRevision == 0  -> a Kubernetes reflector reconnects
//     from its last seen resourceVersion, kine serves it, and the events lost
//     during the drop are never delivered and never reported.
//
// No measurement in this suite ever exercised the drop path, because every
// watcher drained fast enough. This forces it: the watcher stops reading
// entirely while a writer floods, then resumes and reports what it saw.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:2379", "kine endpoint")
	stall := flag.Duration("stall", 15*time.Second, "how long the watcher refuses to read")
	writes := flag.Int("writes", 20000, "writes issued while the watcher is stalled")
	valueSz := flag.Int("value-bytes", 2048, "payload size")
	// The flood MUST target the same prefix the watcher watches. kine filters
	// per watcher AFTER the broadcaster fan-out (sqllog.Watch), so events for a
	// different prefix are drained and discarded by the filtering goroutine and
	// never pressure the subscriber's buffer. A first attempt flooded a
	// different prefix and unsurprisingly produced no drop.
	writerOnly := flag.Bool("writer-only", false, "just flood the watched prefix and exit")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *stall+120*time.Second)
	defer cancel()

	if *writerOnly {
		w, err := clientv3.New(clientv3.Config{Endpoints: []string{*endpoint}, DialTimeout: 10 * time.Second})
		if err != nil {
			fmt.Fprintln(os.Stderr, "writer dial:", err)
			os.Exit(1)
		}
		defer w.Close()
		v := make([]byte, *valueSz)
		for i := range v {
			v[i] = byte('a' + i%26)
		}
		n := 0
		for i := 0; i < *writes; i++ {
			if _, err := w.Put(ctx, fmt.Sprintf("/registry/drop/k%d", i), string(v)); err != nil {
				break
			}
			n++
		}
		fmt.Printf("writer-only: %d writes to /registry/drop/\n", n)
		return
	}

	wc, err := clientv3.New(clientv3.Config{Endpoints: []string{*endpoint}, DialTimeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintln(os.Stderr, "watcher dial:", err)
		os.Exit(1)
	}
	defer wc.Close()

	prefix := "/registry/drop/"
	ch := wc.Watch(ctx, prefix, clientv3.WithPrefix())

	// Read exactly one response so the watch is definitely established, then
	// stop reading. Everything after this backs up: the client's stream buffer,
	// then kine's per-watcher channel, then the broadcaster's subscriber
	// channel — which is where the drop happens.
	writer, err := clientv3.New(clientv3.Config{Endpoints: []string{*endpoint}, DialTimeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintln(os.Stderr, "writer dial:", err)
		os.Exit(1)
	}
	defer writer.Close()

	val := make([]byte, *valueSz)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	if _, err := writer.Put(ctx, prefix+"warmup", "x"); err != nil {
		fmt.Fprintln(os.Stderr, "warmup put:", err)
		os.Exit(1)
	}
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		fmt.Fprintln(os.Stderr, "watch never delivered the warmup event")
		os.Exit(1)
	}
	fmt.Println("watch established; the watcher now stops reading")

	done := make(chan int)
	go func() {
		n := 0
		for i := 0; i < *writes; i++ {
			if _, err := writer.Put(ctx, fmt.Sprintf("%sk%d", prefix, i), string(val)); err != nil {
				break
			}
			n++
		}
		done <- n
	}()

	time.Sleep(*stall)
	written := 0
	select {
	case written = <-done:
	default:
	}
	fmt.Printf("stalled %s; writes issued so far: %d\n", *stall, written)

	// Resume reading and report exactly what the client is told.
	var (
		events   int
		canceled bool
		compact  int64
		reason   string
		closedCh bool
		firstRev int64
		lastRev  int64
		gapAfter int64
	)
	deadline := time.After(20 * time.Second)
readLoop:
	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				closedCh = true
				break readLoop
			}
			if resp.Canceled {
				canceled = true
				compact = resp.CompactRevision
				reason = resp.Err().Error()
				break readLoop
			}
			for _, ev := range resp.Events {
				if firstRev == 0 {
					firstRev = ev.Kv.ModRevision
				}
				if lastRev != 0 && ev.Kv.ModRevision != lastRev+1 && gapAfter == 0 {
					gapAfter = lastRev
				}
				lastRev = ev.Kv.ModRevision
				events++
			}
		case <-deadline:
			break readLoop
		}
	}

	fmt.Println()
	fmt.Println("=== what the client observed ===")
	fmt.Printf("events delivered after resuming : %d\n", events)
	fmt.Printf("revision range seen             : %d..%d\n", firstRev, lastRev)
	fmt.Printf("first revision gap after        : %d\n", gapAfter)
	fmt.Printf("watch channel closed by client  : %v\n", closedCh)
	fmt.Printf("Canceled received               : %v\n", canceled)
	fmt.Printf("CompactRevision on cancel       : %d\n", compact)
	fmt.Printf("CancelReason                    : %q\n", reason)
	fmt.Println()
	switch {
	case canceled && compact != 0:
		fmt.Println("VERDICT: correct — the client is told its position is invalid and will relist.")
	case canceled && compact == 0:
		fmt.Println("VERDICT: DEFECT CONFIRMED — cancelled with CompactRevision 0.")
		fmt.Println("  A reflector reconnects from its last resourceVersion, kine serves it,")
		fmt.Println("  and the events lost during the drop are never delivered or reported.")
	case closedCh:
		fmt.Println("VERDICT: the channel closed with no cancel response reaching the client.")
	default:
		fmt.Println("VERDICT: no drop occurred — the buffers absorbed the stall.")
		fmt.Println("  Increase -writes or -stall, or the drop path is not reachable this way.")
	}
}
