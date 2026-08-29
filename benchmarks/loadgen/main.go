// kine watch-churn load generator.
//
// WHY THIS EXISTS: k0smotron's published benchmark records 9-15% write
// errors for PostgreSQL under watch churn (k0smotron docs/benchmarks.md:26).
// Both Kamaji and k0smotron reach Postgres through kine, so that number
// gates whether a shared-Postgres control-plane density model is viable at
// all. This reproduces the shape of that load against kine's etcd v3 API so
// the figure can be measured rather than inherited.
//
// THE SHAPE: N watchers holding open streams over overlapping prefixes while
// M writers PUT continuously. That combination is the point — kine's watch
// path is a poll loop (pkg/logstructured/sqllog/sql.go:477, 1s ticker at
// :486) issuing `SELECT ... WHERE id > rev`, which contends with concurrent
// INSERTs. Writers alone or watchers alone do not reproduce it.
//
// WHAT IT MEASURES, and what it deliberately does not: PUT outcomes are
// counted by result, not sampled, so the error RATE is exact. Latency is
// recorded per operation and reported as percentiles from the full set, not
// a running estimate — at these volumes the memory cost is trivial and an
// approximated p99 is the number most likely to be wrong in the direction
// that flatters the result. Watch delivery latency is measured end to end
// (write timestamp embedded in the value, compared on receipt) because the
// wake-up latency is exactly what LISTEN/NOTIFY is supposed to fix.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Default: record one delivery latency per N events, so the consumer cannot
// become the bottleneck at saturation. At LOW event volumes this default is a
// trap — a 60 s run at 4 puts/s yields ~9 samples, and p95/p99 computed from 9
// samples come out identical to p50 and look like a confident result. Set
// -latency-sample-every 1 for any low-rate profile.
const defaultLatencySampleEvery = 50

type result struct {
	Label            string   `json:"label"`
	Endpoint         string   `json:"endpoint"`
	Writers          int      `json:"writers"`
	Watchers         int      `json:"watchers"`
	DurationSec      float64  `json:"duration_sec"`
	ValueBytes       int      `json:"value_bytes"`
	Keyspace         int      `json:"keyspace"`
	WriteRateCap     int      `json:"write_rate_cap_per_writer"`
	LatencySamples   int      `json:"watch_latency_samples"`
	PutsOK           int64    `json:"puts_ok"`
	PutsErr          int64    `json:"puts_err"`
	PutsConflict     int64    `json:"puts_conflict"`
	PutErrorRatePct  float64  `json:"put_error_rate_pct"`
	PutRatePerSec    float64  `json:"put_rate_per_sec"`
	PutP50Ms         float64  `json:"put_p50_ms"`
	PutP95Ms         float64  `json:"put_p95_ms"`
	PutP99Ms         float64  `json:"put_p99_ms"`
	PutMaxMs         float64  `json:"put_max_ms"`
	WatchEvents      int64    `json:"watch_events"`
	WatchClosedEarly int64    `json:"watch_closed_early"`
	WatchCancels     int64    `json:"watch_cancels"`
	WatchErrs        []string `json:"watch_errors,omitempty"`
	WatchLastEvP50   float64  `json:"watch_last_event_p50_sec"`
	WatchLastEvMax   float64  `json:"watch_last_event_max_sec"`
	WatchP50Ms       float64  `json:"watch_delivery_p50_ms"`
	WatchP95Ms       float64  `json:"watch_delivery_p95_ms"`
	WatchP99Ms       float64  `json:"watch_delivery_p99_ms"`
	FirstErrors      []string `json:"first_errors,omitempty"`
	StartedAt        string   `json:"started_at"`
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func main() {
	var (
		endpoint = flag.String("endpoint", "localhost:2379", "kine etcd endpoint")
		writers  = flag.Int("writers", 16, "concurrent writers")
		watchers = flag.Int("watchers", 64, "concurrent watchers")
		dur      = flag.Duration("duration", 60*time.Second, "run duration")
		valueSz  = flag.Int("value-bytes", 2048, "payload size per key")
		keyspace = flag.Int("keyspace", 500, "distinct keys per writer")
		rateCap  = flag.Int("write-rate", 0, "per-writer puts/sec cap (0 = unthrottled)")
		label    = flag.String("label", "baseline", "run label")
		out      = flag.String("out", "", "write JSON result here")
		// A clientv3 watch with no WithRev sends StartRevision 0. kine passes
		// that straight to logstructured.Watch, which calls After(key, end, 0, 0)
		// — an UNLIMITED read of the whole table before the watch streams. On an
		// aged table that materialises every row through RowsToEvents/bytes.Clone.
		// This flag starts watches at the CURRENT revision instead, which is what
		// a Kubernetes reflector does after its initial LIST.
		fromCurrent = flag.Bool("watch-from-current", false, "start watches at the current revision instead of 0")
		// Watchers can be pointed at a SECOND kine instance sharing the same
		// Postgres. kine signals its poll loop in-process on every insert
		// (sql.go:660), so a single instance wakes its watchers in
		// milliseconds — but that signal never crosses the process boundary.
		// In an HA topology a write on instance A leaves instance B's watchers
		// waiting for B's 1 s fallback ticker. This flag measures that gap.
		sampleEvery   = flag.Int("latency-sample-every", defaultLatencySampleEvery, "record 1 delivery latency per N events; use 1 for low-rate runs")
		watchEndpoint = flag.String("watch-endpoint", "", "separate endpoint for watchers (default: -endpoint)")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Watchers first, so their poll loops are already running when the write
	// load starts — the contention under test is watch-vs-write, and starting
	// them in the other order measures a quieter system for the first seconds.
	var (
		watchEvents  int64
		watchClosed  int64 // watch channels that ended before we cancelled
		watchCancels int64 // responses carrying Canceled
		watchMu      sync.Mutex
		watchLat     []float64
		watchErrs    []string
		lastSeen     []float64 // per watcher: seconds from start to its LAST event
		wg           sync.WaitGroup
	)
	loadStart := time.Now()
	wEndpoint := *endpoint
	if *watchEndpoint != "" {
		wEndpoint = *watchEndpoint
	}
	// ONE revision lookup shared by every watcher. Doing it per watcher issued
	// 256 LIST queries against the aged table at startup, each logged by kine as
	// Slow SQL, and the run never completed — which then read as a flatteringly
	// low memory figure rather than as the failure it was.
	var startRev int64
	if *fromCurrent {
		rc, rerr := clientv3.New(clientv3.Config{Endpoints: []string{wEndpoint}, DialTimeout: 10 * time.Second})
		if rerr != nil {
			fmt.Fprintf(os.Stderr, "revision lookup dial: %v\n", rerr)
			os.Exit(1)
		}
		gr, gerr := rc.Get(ctx, "/registry/bench/", clientv3.WithPrefix(), clientv3.WithLimit(1), clientv3.WithKeysOnly())
		rc.Close()
		if gerr != nil {
			fmt.Fprintf(os.Stderr, "revision lookup: %v\n", gerr)
			os.Exit(1)
		}
		startRev = gr.Header.Revision
		fmt.Fprintf(os.Stderr, "watching from revision %d\n", startRev)
	}
	watchClients := make([]*clientv3.Client, 0, *watchers)
	for i := 0; i < *watchers; i++ {
		c, err := clientv3.New(clientv3.Config{Endpoints: []string{wEndpoint}, DialTimeout: 10 * time.Second})
		if err != nil {
			fmt.Fprintf(os.Stderr, "watcher %d dial: %v\n", i, err)
			os.Exit(1)
		}
		watchClients = append(watchClients, c)
		// Overlapping prefixes on purpose: real control planes have many
		// watchers over the same resource space, which is what makes the
		// poll loop expensive.
		prefix := fmt.Sprintf("/registry/bench/w%d/", i%8)
		wopts := []clientv3.OpOption{clientv3.WithPrefix()}
		if *fromCurrent {
			wopts = append(wopts, clientv3.WithRev(startRev))
		}
		wch := c.Watch(ctx, prefix, wopts...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// SAMPLED on purpose. The first version parsed a timestamp and
			// took a mutex for EVERY event, which made the consumer itself
			// the bottleneck: watchers received 0.5% of expected events and
			// the "latency" measured was the harness queueing, not kine.
			// Sampling keeps the consumer cheap so the number means what it
			// says. Events are still counted in full.
			local := make([]float64, 0, 4096)
			seen := 0
			// When a watcher goes quiet, the event COUNT alone cannot tell
			// "delivered slowly" from "kine unsubscribed us and never said
			// so" — broadcaster.go:66 drops the event and unsubs a slow
			// consumer non-blockingly, and the gRPC stream stays open and
			// mute. Recording WHEN the last event arrived separates the two.
			var lastEvent time.Time
			for resp := range wch {
				// A watch that STOPS is invisible in an event count — the
				// number just comes out low and reads like slow delivery.
				// Record why a stream ended, or the watch measurement cannot
				// be distinguished from a broken harness.
				if resp.Canceled || resp.Err() != nil {
					atomic.AddInt64(&watchCancels, 1)
					watchMu.Lock()
					if len(watchErrs) < 5 && resp.Err() != nil {
						watchErrs = append(watchErrs, resp.Err().Error())
					}
					watchMu.Unlock()
				}
				now := time.Now().UnixNano()
				n := len(resp.Events)
				atomic.AddInt64(&watchEvents, int64(n))
				for _, ev := range resp.Events {
					seen++
					if seen%*sampleEvery != 0 || len(ev.Kv.Value) < 19 {
						continue
					}
					sent, err := strconv.ParseInt(string(ev.Kv.Value[:19]), 10, 64)
					if err == nil && sent > 0 {
						local = append(local, float64(now-sent)/1e6)
					}
				}
				lastEvent = time.Now()
			}
			if !lastEvent.IsZero() {
				watchMu.Lock()
				lastSeen = append(lastSeen, lastEvent.Sub(loadStart).Seconds())
				watchMu.Unlock()
			}
			if ctx.Err() == nil {
				atomic.AddInt64(&watchClosed, 1) // ended on its own, not by us
			}
			watchMu.Lock()
			watchLat = append(watchLat, local...)
			watchMu.Unlock()
		}()
	}
	defer func() {
		for _, c := range watchClients {
			_ = c.Close()
		}
	}()
	time.Sleep(2 * time.Second) // let watch streams establish

	var (
		putsOK, putsErr int64
		putsConflict    int64
		latMu           sync.Mutex
		putLat          []float64
		errMu           sync.Mutex
		firstErrs       []string
	)
	// PAYLOAD COMPRESSIBILITY IS A LOAD PARAMETER, not a detail. The first
	// version filled values with a repeated byte, which Postgres' default
	// EXTENDED storage compresses roughly 20:1 — so the benchmark wrote a
	// twentieth of the bytes it claimed to, and E3 (SET STORAGE EXTERNAL)
	// measured a 21x table blowup that said more about the filler than about
	// TOAST. Kubernetes payloads are protobuf and compress poorly, so the
	// representative filler is incompressible. Seeded, so runs stay
	// reproducible and comparable.
	filler := make([]byte, *valueSz)
	rnd := rand.New(rand.NewSource(int64(*valueSz)))
	rnd.Read(filler)

	started := time.Now()
	deadline := started.Add(*dur)
	var wwg sync.WaitGroup
	for w := 0; w < *writers; w++ {
		c, err := clientv3.New(clientv3.Config{Endpoints: []string{*endpoint}, DialTimeout: 10 * time.Second})
		if err != nil {
			fmt.Fprintf(os.Stderr, "writer %d dial: %v\n", w, err)
			os.Exit(1)
		}
		wwg.Add(1)
		go func(w int, c *clientv3.Client) {
			defer wwg.Done()
			defer c.Close()
			// A per-writer rate cap matters for realism: an unthrottled
			// writer produces ~14k puts/sec, which no real control plane
			// does. Modelling N tenants means N writers at a plausible
			// per-cluster rate, not a saturation test mislabelled as one.
			var tick *time.Ticker
			if *rateCap > 0 {
				tick = time.NewTicker(time.Second / time.Duration(*rateCap))
				defer tick.Stop()
			}
			// WRITE THE WAY KUBERNETES WRITES. The apiserver never issues a
			// bare Put for a key that exists: every update is a transaction
			// comparing the key's ModRevision, every create compares
			// CreateRevision to 0. That distinction is not cosmetic. With bare
			// Puts, kine derives prev_revision from a revision snapshot its 1 s
			// poll loop has not yet advanced, so rewriting an existing key is
			// attempted as a CREATE, collides with the (name, prev_revision)
			// unique index, and is DISCARDED — while Put still returns success.
			// Measured on this harness: 200 bare puts over 10 keys persisted 29
			// rows, and the missing 171 were invisible in every client-side
			// metric. A benchmark built on bare Puts measures a write path no
			// real cluster uses and silently over-reports its own throughput.
			//
			// Each writer owns a disjoint key range, so the local revision map
			// is authoritative and a conflict means a genuine lost update.
			knownRev := make(map[string]int64, *keyspace)
			n := 0
			for time.Now().Before(deadline) {
				if tick != nil {
					<-tick.C
				}
				key := fmt.Sprintf("/registry/bench/w%d/w%dkey%d", w%8, w, n%*keyspace)
				val := fmt.Sprintf("%019d", time.Now().UnixNano()) + string(filler)
				// kine accepts exactly one transaction shape (server/update.go:10):
				// a single ModRevision compare, a Put on success, and a Range on
				// failure. ModRevision 0 means "must not exist", which is how a
				// create is expressed. Any other shape is rejected outright with
				// "unsupported operations in txn request" — this is a restricted
				// etcd, not a general one.
				t0 := time.Now()
				resp, err := c.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision(key), "=", knownRev[key])).
					Then(clientv3.OpPut(key, val)).
					Else(clientv3.OpGet(key)).
					Commit()
				ms := float64(time.Since(t0).Microseconds()) / 1000.0
				if err == nil && !resp.Succeeded {
					// Our revision view is stale. Count it as a conflict, never
					// as a success, and resync from the Range the failure branch
					// already returned — no extra round trip.
					atomic.AddInt64(&putsConflict, 1)
					knownRev[key] = 0
					if len(resp.Responses) > 0 {
						if rr := resp.Responses[0].GetResponseRange(); rr != nil && len(rr.Kvs) > 0 {
							knownRev[key] = rr.Kvs[0].ModRevision
						}
					}
					n++
					continue
				}
				if err == nil {
					knownRev[key] = resp.Header.Revision
				}
				if err != nil {
					atomic.AddInt64(&putsErr, 1)
					errMu.Lock()
					if len(firstErrs) < 5 {
						firstErrs = append(firstErrs, err.Error())
					}
					errMu.Unlock()
				} else {
					atomic.AddInt64(&putsOK, 1)
					latMu.Lock()
					putLat = append(putLat, ms)
					latMu.Unlock()
				}
				n++
			}
		}(w, c)
	}
	wwg.Wait()
	elapsed := time.Since(started).Seconds()

	// Drain briefly so in-flight watch events land before we report.
	time.Sleep(3 * time.Second)
	cancel()
	wg.Wait()

	sort.Float64s(putLat)
	lastSeenSorted := append([]float64(nil), lastSeen...)
	sort.Float64s(lastSeenSorted)
	watchMu.Lock()
	sort.Float64s(watchLat)
	wl := watchLat
	watchMu.Unlock()

	total := putsOK + putsErr
	rate := 0.0
	if total > 0 {
		rate = float64(putsErr) / float64(total) * 100
	}
	r := result{
		Label: *label, Endpoint: *endpoint, Writers: *writers, Watchers: *watchers,
		DurationSec: elapsed, ValueBytes: *valueSz, Keyspace: *keyspace, WriteRateCap: *rateCap,
		PutsOK: putsOK, PutsErr: putsErr, PutsConflict: atomic.LoadInt64(&putsConflict),
		PutErrorRatePct: rate,
		PutRatePerSec:   float64(putsOK) / elapsed,
		PutP50Ms:        pct(putLat, 0.50), PutP95Ms: pct(putLat, 0.95),
		PutP99Ms: pct(putLat, 0.99), PutMaxMs: pct(putLat, 1.0),
		WatchEvents:      atomic.LoadInt64(&watchEvents),
		WatchClosedEarly: atomic.LoadInt64(&watchClosed),
		WatchCancels:     atomic.LoadInt64(&watchCancels),
		WatchErrs:        watchErrs,
		LatencySamples:   len(wl),
		WatchLastEvP50:   pct(lastSeenSorted, 0.50),
		WatchLastEvMax:   pct(lastSeenSorted, 1.0),
		WatchP50Ms:       pct(wl, 0.50), WatchP95Ms: pct(wl, 0.95), WatchP99Ms: pct(wl, 0.99),
		FirstErrors: firstErrs,
		StartedAt:   started.UTC().Format(time.RFC3339),
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", *out, err)
			os.Exit(1)
		}
	}
}
