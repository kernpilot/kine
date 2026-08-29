// Tier-2 load generator: drives a REAL kube-apiserver that is backed by kine.
//
// WHY THIS EXISTS. Every tier-1 number in this suite comes from a synthetic
// etcd client, and the single largest defect found while building that harness
// was writing in a shape the apiserver never uses (see benchmarks/kine/README).
// A synthetic client can be wrong about the workload in ways no amount of
// internal consistency reveals. This driver removes that whole class of doubt:
// it speaks the Kubernetes REST API, and the apiserver generates the etcd
// transactions itself. If a tier-1 gain does not survive here, it was not real.
//
// WHAT IT DOES. Each writer owns a disjoint set of ConfigMaps and rewrites them
// in a loop, carrying the resourceVersion the apiserver returned from its last
// write — which is exactly how a controller behaves, and exercises the
// optimistic-concurrency path rather than a blind overwrite. A 409 is counted
// as a conflict, never as a success. Watchers hold streaming watches on the
// collection and measure end-to-end delivery latency from a timestamp embedded
// in the object.
package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Label          string   `json:"label"`
	Writers        int      `json:"writers"`
	Watchers       int      `json:"watchers"`
	DurationSec    float64  `json:"duration_sec"`
	ObjectsPerW    int      `json:"objects_per_writer"`
	ValueBytes     int      `json:"value_bytes"`
	WritesOK       int64    `json:"writes_ok"`
	WritesErr      int64    `json:"writes_err"`
	WritesConflict int64    `json:"writes_conflict"`
	ErrRatePct     float64  `json:"write_error_rate_pct"`
	ConflictPct    float64  `json:"write_conflict_pct"`
	RatePerSec     float64  `json:"write_rate_per_sec"`
	P50Ms          float64  `json:"write_p50_ms"`
	P95Ms          float64  `json:"write_p95_ms"`
	P99Ms          float64  `json:"write_p99_ms"`
	WatchEvents    int64    `json:"watch_events"`
	WatchSamples   int      `json:"watch_latency_samples"`
	WatchP50Ms     float64  `json:"watch_delivery_p50_ms"`
	WatchP95Ms     float64  `json:"watch_delivery_p95_ms"`
	WatchP99Ms     float64  `json:"watch_delivery_p99_ms"`
	FirstErrors    []string `json:"first_errors,omitempty"`
}

func pct(s []float64, p float64) float64 {
	if len(s) == 0 {
		return 0
	}
	return s[int(float64(len(s)-1)*p)]
}

type client struct {
	base  string
	token string
	http  *http.Client
}

func (c *client) do(method, path string, body []byte) (int, []byte, error) {
	var r *http.Request
	var err error
	if body != nil {
		r, err = http.NewRequest(method, c.base+path, bytes.NewReader(body))
	} else {
		r, err = http.NewRequest(method, c.base+path, nil)
	}
	if err != nil {
		return 0, nil, err
	}
	r.Header.Set("Authorization", "Bearer "+c.token)
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(r)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.Bytes(), nil
}

func newClient(base, token string) *client {
	return &client{base: base, token: token, http: &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 512,
		},
	}}
}

type objMeta struct {
	Metadata struct {
		Name            string `json:"name"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Data map[string]string `json:"data"`
}

func main() {
	var (
		server    = flag.String("server", "https://127.0.0.1:6443", "apiserver for writes")
		watchSrv  = flag.String("watch-server", "", "apiserver for watches (default: -server)")
		token     = flag.String("token", "benchtoken123", "bearer token")
		ns        = flag.String("namespace", "bench", "namespace to use")
		writers   = flag.Int("writers", 8, "concurrent writers")
		watchers  = flag.Int("watchers", 8, "concurrent watchers")
		objects   = flag.Int("objects", 20, "configmaps per writer")
		dur       = flag.Duration("duration", 60*time.Second, "run duration")
		valueSz   = flag.Int("value-bytes", 2048, "payload bytes per object")
		rateCap   = flag.Int("write-rate", 0, "per-writer writes/sec (0 = unthrottled)")
		label     = flag.String("label", "t2", "run label")
		out       = flag.String("out", "", "write JSON result here")
		sampleEvy = flag.Int("latency-sample-every", 1, "record 1 delivery latency per N events")
	)
	flag.Parse()

	wsrv := *server
	if *watchSrv != "" {
		wsrv = *watchSrv
	}

	admin := newClient(*server, *token)
	// Namespace may already exist from a previous run; 409 is fine.
	_, _, _ = admin.do("POST", "/api/v1/namespaces",
		[]byte(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"`+*ns+`"}}`))

	// Incompressible payload, for the same reason as the tier-1 generator: a
	// repeated byte compresses ~20:1 in Postgres and quietly shrinks the real
	// write volume to a fraction of what the run claims to produce.
	filler := make([]byte, *valueSz)
	rnd := rand.New(rand.NewSource(int64(*valueSz)))
	rnd.Read(filler)
	fillStr := fmt.Sprintf("%x", filler)[:*valueSz]

	var (
		watchEvents int64
		watchMu     sync.Mutex
		watchLat    []float64
		wg          sync.WaitGroup
		stop        = make(chan struct{})
	)

	// Establish watches BEFORE any load, so they observe the whole run.
	startRV := "0"
	if code, body, err := newClient(wsrv, *token).do("GET", "/api/v1/namespaces/"+*ns+"/configmaps?limit=1", nil); err == nil && code == 200 {
		var l struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
		}
		if json.Unmarshal(body, &l) == nil {
			startRV = l.Metadata.ResourceVersion
		}
	}

	for i := 0; i < *watchers; i++ {
		wc := newClient(wsrv, *token)
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]float64, 0, 4096)
			seen := 0
			req, err := http.NewRequest("GET",
				wsrv+"/api/v1/namespaces/"+*ns+"/configmaps?watch=true&resourceVersion="+startRV, nil)
			if err != nil {
				return
			}
			req.Header.Set("Authorization", "Bearer "+*token)
			resp, err := wc.http.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(make([]byte, 0, 1<<20), 8<<20)
			done := false
			go func() { <-stop; done = true; resp.Body.Close() }()
			for sc.Scan() {
				if done {
					break
				}
				now := time.Now().UnixNano()
				var ev struct {
					Object objMeta `json:"object"`
				}
				if json.Unmarshal(sc.Bytes(), &ev) != nil {
					continue
				}
				atomic.AddInt64(&watchEvents, 1)
				seen++
				if seen%*sampleEvy != 0 {
					continue
				}
				if ts, ok := ev.Object.Data["ts"]; ok {
					if sent, err := strconv.ParseInt(ts, 10, 64); err == nil && sent > 0 {
						local = append(local, float64(now-sent)/1e6)
					}
				}
			}
			watchMu.Lock()
			watchLat = append(watchLat, local...)
			watchMu.Unlock()
		}()
	}
	time.Sleep(3 * time.Second)

	var (
		ok, errs, conflicts int64
		latMu               sync.Mutex
		lat                 []float64
		errMu               sync.Mutex
		firstErrs           []string
	)

	started := time.Now()
	deadline := started.Add(*dur)
	var wwg sync.WaitGroup
	for w := 0; w < *writers; w++ {
		wwg.Add(1)
		go func(w int) {
			defer wwg.Done()
			c := newClient(*server, *token)
			rv := map[string]string{}
			var tick *time.Ticker
			if *rateCap > 0 {
				tick = time.NewTicker(time.Second / time.Duration(*rateCap))
				defer tick.Stop()
			}
			n := 0
			for time.Now().Before(deadline) {
				if tick != nil {
					<-tick.C
				}
				name := fmt.Sprintf("bench-w%d-%d", w, n%*objects)
				n++
				body := map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{"name": name, "namespace": *ns},
					"data":     map[string]string{"ts": strconv.FormatInt(time.Now().UnixNano(), 10), "payload": fillStr},
				}
				method, path := "POST", "/api/v1/namespaces/"+*ns+"/configmaps"
				if cur, seen := rv[name]; seen {
					// Carry the resourceVersion forward, as a controller does.
					// This is the optimistic-concurrency path; a blind write
					// would measure a path Kubernetes does not take.
					body["metadata"].(map[string]any)["resourceVersion"] = cur
					method, path = "PUT", path+"/"+name
				}
				raw, _ := json.Marshal(body)
				t0 := time.Now()
				code, resp, err := c.do(method, path, raw)
				ms := float64(time.Since(t0).Microseconds()) / 1000.0
				switch {
				case err != nil:
					atomic.AddInt64(&errs, 1)
					errMu.Lock()
					if len(firstErrs) < 5 {
						firstErrs = append(firstErrs, err.Error())
					}
					errMu.Unlock()
				case code == 409:
					// Conflict or already-exists: resync from the server.
					atomic.AddInt64(&conflicts, 1)
					if _, g, gerr := c.do("GET", "/api/v1/namespaces/"+*ns+"/configmaps/"+name, nil); gerr == nil {
						var o objMeta
						if json.Unmarshal(g, &o) == nil && o.Metadata.ResourceVersion != "" {
							rv[name] = o.Metadata.ResourceVersion
						}
					}
				case code >= 200 && code < 300:
					var o objMeta
					if json.Unmarshal(resp, &o) == nil {
						rv[name] = o.Metadata.ResourceVersion
					}
					atomic.AddInt64(&ok, 1)
					latMu.Lock()
					lat = append(lat, ms)
					latMu.Unlock()
				default:
					atomic.AddInt64(&errs, 1)
					errMu.Lock()
					if len(firstErrs) < 5 {
						firstErrs = append(firstErrs, fmt.Sprintf("HTTP %d: %s", code, strings.TrimSpace(string(resp))[:min(160, len(strings.TrimSpace(string(resp))))]))
					}
					errMu.Unlock()
				}
			}
		}(w)
	}
	wwg.Wait()
	elapsed := time.Since(started).Seconds()

	time.Sleep(3 * time.Second) // let in-flight watch events land
	close(stop)
	wg.Wait()

	sort.Float64s(lat)
	watchMu.Lock()
	sort.Float64s(watchLat)
	wl := watchLat
	watchMu.Unlock()

	total := ok + errs + conflicts
	r := result{
		Label: *label, Writers: *writers, Watchers: *watchers, DurationSec: elapsed,
		ObjectsPerW: *objects, ValueBytes: *valueSz,
		WritesOK: ok, WritesErr: errs, WritesConflict: conflicts,
		ErrRatePct:  ratio(errs, total),
		ConflictPct: ratio(conflicts, total),
		RatePerSec:  float64(ok) / elapsed,
		P50Ms:       pct(lat, 0.50), P95Ms: pct(lat, 0.95), P99Ms: pct(lat, 0.99),
		WatchEvents: atomic.LoadInt64(&watchEvents), WatchSamples: len(wl),
		WatchP50Ms: pct(wl, 0.50), WatchP95Ms: pct(wl, 0.95), WatchP99Ms: pct(wl, 0.99),
		FirstErrors: firstErrs,
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

func ratio(a, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(a) / float64(total) * 100
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
