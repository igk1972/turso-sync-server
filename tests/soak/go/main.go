// Soak client on the Go driver: the load and checks of soak.ts, run by its
// own writers in the same database. Tunables are SOAK_* environment variables.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	turso "turso.tech/database/tursogo"
)

func setting(name string, fallback float64) float64 {
	if value, ok := os.LookupEnv("SOAK_" + name); ok {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Fatalf("SOAK_%s: %v", name, err)
		}
		return parsed
	}
	return fallback
}

func text(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

var (
	syncURL       = text("SOAK_URL", "http://127.0.0.1:8000")
	token         = os.Getenv("TURSO_SYNC_AUTH_TOKEN")
	backupURL     = os.Getenv("SOAK_BACKUP_URL")
	serverMetrics = text("SOAK_SERVER_METRICS", "http://127.0.0.1:9464/metrics")
	metricsPort   = int(setting("METRICS_PORT", 9466))
	writerCount   = int(setting("WRITERS", 3))
	readerCount   = int(setting("READERS", 3))
	// Writer numbers start past the ones soak.ts uses, so rows never collide.
	firstWriter     = int64(setting("FIRST_WRITER", 11))
	writeInterval   = seconds(setting("WRITE_INTERVAL_MS", 2000) / 1000)
	pullInterval    = seconds(setting("PULL_INTERVAL_MS", 1000) / 1000)
	rowBytes        = int(setting("ROW_BYTES", 1024))
	window          = int64(setting("WINDOW", 1000))
	burstRows       = int64(setting("BURST_ROWS", 200))
	burstInterval   = seconds(setting("BURST_INTERVAL_S", 600))
	bootstrapEvery  = seconds(setting("BOOTSTRAP_INTERVAL_S", 60))
	chunkedEvery    = int(setting("CHUNKED_EVERY", 10))
	compareInterval = seconds(setting("COMPARE_INTERVAL_S", 1800))
	restoreInterval = seconds(setting("RESTORE_INTERVAL_S", 10800))
	// soak.ts restores 10 minutes in; two restores at once would not fit the pod.
	firstRestore     = seconds(setting("FIRST_RESTORE_S", 5400))
	backupLag        = seconds(setting("BACKUP_LAG_S", 30))
	statusInterval   = seconds(setting("STATUS_INTERVAL_S", 300))
	clientWalBytes   = int64(setting("CLIENT_WAL_MB", 4) * (1 << 20))
	operationTimeout = seconds(setting("OPERATION_TIMEOUT_S", 300))
)

func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// Writer w owns rows w*idSpan+1, +2, …
const idSpan = 1_000_000_000_000

// Metrics in the Prometheus text format.

var buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800}

type family struct {
	name, kind, help string
	series           map[string]float64
}

var (
	metricsMu sync.Mutex
	families  []*family
	byName    = map[string]*family{}
)

func defineFamily(name, kind, help string) {
	f := &family{name: name, kind: kind, help: help, series: map[string]float64{}}
	families = append(families, f)
	byName[name] = f
}

func init() {
	defineFamily("soak_operations_total", "counter", "Client operations and checks by result")
	defineFamily("soak_operation_seconds", "histogram", "Time taken by client operations and checks")
	defineFamily("soak_violations_total", "counter", "Consistency violations by check")
	defineFamily("soak_rows_acked", "gauge", "Last row of each writer acknowledged by the server")
	defineFamily("soak_last_ok_timestamp_seconds", "gauge", "When a comparison or a restore check last passed")
	defineFamily("process_resident_memory_bytes", "gauge", "Resident memory size")
	defineFamily("process_cpu_seconds_total", "counter", "User and system CPU time spent")
	defineFamily("go_memstats_heap_inuse_bytes", "gauge", "Go heap in use; the rest of the resident memory is mostly native")
	defineFamily("go_goroutines", "gauge", "Goroutines that currently exist")
}

type labels [][2]string

func seriesKey(name string, l labels, suffix string) string {
	parts := make([]string, len(l))
	for i, pair := range l {
		parts[i] = fmt.Sprintf("%s=%q", pair[0], pair[1])
	}
	return name + suffix + "{" + strings.Join(parts, ",") + "}"
}

func add(name string, l labels, by float64, suffix string) {
	metricsMu.Lock()
	defer metricsMu.Unlock()
	byName[name].series[seriesKey(name, l, suffix)] += by
}

func set(name string, l labels, value float64) {
	metricsMu.Lock()
	defer metricsMu.Unlock()
	byName[name].series[seriesKey(name, l, "")] = value
}

func observe(op string, elapsed float64) {
	for _, le := range buckets {
		hit := 0.0
		if elapsed <= le {
			hit = 1
		}
		add("soak_operation_seconds", labels{{"op", op}, {"le", strconv.FormatFloat(le, 'g', -1, 64)}}, hit, "_bucket")
	}
	add("soak_operation_seconds", labels{{"op", op}, {"le", "+Inf"}}, 1, "_bucket")
	add("soak_operation_seconds", labels{{"op", op}}, elapsed, "_sum")
	add("soak_operation_seconds", labels{{"op", op}}, 1, "_count")
}

// residentBytes reads the current RSS on Linux; elsewhere it falls back to the peak.
func residentBytes() float64 {
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if fields := strings.Fields(line); len(fields) == 3 && fields[0] == "VmRSS:" {
				kb, _ := strconv.ParseFloat(fields[1], 64)
				return kb * 1024
			}
		}
	}
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	if runtime.GOOS == "darwin" {
		return float64(usage.Maxrss)
	}
	return float64(usage.Maxrss) * 1024
}

func exposition() string {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	set("process_resident_memory_bytes", nil, residentBytes())
	set("process_cpu_seconds_total", nil,
		time.Duration(usage.Utime.Nano()+usage.Stime.Nano()).Seconds())
	set("go_memstats_heap_inuse_bytes", nil, float64(memory.HeapInuse))
	set("go_goroutines", nil, float64(runtime.NumGoroutine()))
	metricsMu.Lock()
	defer metricsMu.Unlock()
	var out strings.Builder
	for _, f := range families {
		fmt.Fprintf(&out, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind)
		keys := make([]string, 0, len(f.series))
		for key := range f.series {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			fmt.Fprintf(&out, "%s %s\n", key, strconv.FormatFloat(f.series[key], 'g', -1, 64))
		}
	}
	return out.String()
}

func total(name, match string) float64 {
	metricsMu.Lock()
	defer metricsMu.Unlock()
	sum := 0.0
	for key, value := range byName[name].series {
		if strings.Contains(key, match) {
			sum += value
		}
	}
	return sum
}

// Logging, with repeated errors of one kind folded into one line a minute.

type errorEntry struct {
	at         time.Time
	suppressed int
}

var (
	errorMu  sync.Mutex
	errorLog = map[string]*errorEntry{}
)

func logError(op string, err error) {
	errorMu.Lock()
	defer errorMu.Unlock()
	entry := errorLog[op]
	if entry != nil && time.Since(entry.at) < time.Minute {
		entry.suppressed++
		return
	}
	more := ""
	if entry != nil && entry.suppressed > 0 {
		more = fmt.Sprintf(" (%d more since the last report)", entry.suppressed)
	}
	log.Printf("error in %s: %v%s", op, err, more)
	errorLog[op] = &errorEntry{at: time.Now()}
}

var violationLines atomic.Int64

func violation(check, format string, args ...any) {
	add("soak_violations_total", labels{{"check", check}}, 1, "")
	if violationLines.Add(1) <= 1000 {
		log.Printf("VIOLATION %s: %s", check, fmt.Sprintf(format, args...))
	}
}

// reported marks errors timed already logged, so forever does not log them twice.
type reported struct{ error }

func (r reported) Unwrap() error { return r.error }

func timed(op string, action func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	start := time.Now()
	err := action(ctx)
	observe(op, time.Since(start).Seconds())
	if err != nil {
		add("soak_operations_total", labels{{"op", op}, {"result", "error"}}, 1, "")
		logError(op, err)
		return reported{err}
	}
	add("soak_operations_total", labels{{"op", op}, {"result", "ok"}}, 1, "")
	return nil
}

func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.5 + rand.Float64()))
}

func retry[T any](op string, action func() (T, error)) T {
	for {
		value, err := action()
		if err == nil {
			return value
		}
		logError(op, err)
		time.Sleep(2 * time.Second)
	}
}

// forever runs step forever, the first time after first to catch a broken setup early.
func forever(op string, pause func() time.Duration, first time.Duration, step func() error) {
	for wait := first; ; wait = pause() {
		time.Sleep(wait)
		if err := step(); err != nil {
			if !errors.As(err, new(reported)) {
				logError(op, err)
			}
		}
	}
}

// Clients.

// TursoSyncDb has no Close in tursogo 0.8.3, and every client left behind
// keeps its whole database in native memory, several megabytes each. The
// soak frees them with the binding the driver never calls.
//
//go:linkname deinitSyncDatabase turso.tech/database/tursogo.turso_sync_database_deinit
func deinitSyncDatabase(turso.TursoSyncDatabase)

func init() {
	field, ok := reflect.TypeFor[turso.TursoSyncDb]().FieldByName("db")
	if !ok || field.Offset != 0 || field.Type != reflect.TypeFor[turso.TursoSyncDatabase]() {
		log.Fatal("tursogo changed the layout of TursoSyncDb; the native database cannot be freed")
	}
}

type client struct {
	name string
	sync *turso.TursoSyncDb
	db   *sql.DB
}

func connect(ctx context.Context, name string, pullBytes int) (*client, error) {
	syncDb, err := turso.NewTursoSyncDb(ctx, turso.TursoSyncDbConfig{
		Path:               ":memory:",
		RemoteUrl:          syncURL,
		AuthToken:          token,
		ClientName:         name,
		PullBytesThreshold: pullBytes,
	})
	if err != nil {
		return nil, err
	}
	db, err := syncDb.Connect(ctx)
	if err != nil {
		return nil, err
	}
	// One connection: statements and syncs of a client never overlap.
	db.SetMaxOpenConns(1)
	return &client{name: name, sync: syncDb, db: db}, nil
}

func (c *client) close() {
	_ = c.db.Close()
	deinitSyncDatabase(*(*turso.TursoSyncDatabase)(unsafe.Pointer(c.sync)))
}

func (c *client) pull(ctx context.Context) error {
	_, err := c.sync.Pull(ctx)
	return err
}

func queryValues(ctx context.Context, db *sql.DB, query string, args ...any) ([]int64, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var xs []int64
	for rows.Next() {
		var x int64
		if err := rows.Scan(&x); err != nil {
			return nil, err
		}
		xs = append(xs, x)
	}
	return xs, rows.Err()
}

func values(ctx context.Context, db *sql.DB) ([]int64, error) {
	return queryValues(ctx, db, "SELECT x FROM t ORDER BY x")
}

// digest covers the rows of this client's writers only: soak.ts keeps
// writing its own while a comparison here holds ours still.
func digest(ctx context.Context, c *client) (string, error) {
	low, high := firstWriter*idSpan, (firstWriter+int64(writerCount))*idSpan
	rows, err := c.db.QueryContext(ctx, "SELECT x, y FROM t WHERE x > ? AND x < ? ORDER BY x", low, high)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	hash := sha256.New()
	for rows.Next() {
		var x int64
		var y string
		if err := rows.Scan(&x, &y); err != nil {
			return "", err
		}
		fmt.Fprintf(hash, "%d:%s;", x, y)
	}
	return hex.EncodeToString(hash.Sum(nil)), rows.Err()
}

// tops returns the last row of each writer. Writes are applied in order, so
// any consistent state holds, per writer, an unbroken run of its last
// min(top, window) rows.
func tops(check string, xs []int64) map[int64]int64 {
	type run struct{ first, last, count int64 }
	runs := map[int64]*run{}
	for _, x := range xs {
		w, seq := x/idSpan, x%idSpan
		if r := runs[w]; r != nil {
			r.last = seq
			r.count++
		} else {
			runs[w] = &run{seq, seq, 1}
		}
	}
	result := map[int64]int64{}
	for w, r := range runs {
		if r.last-r.first+1 != r.count || r.count != min(r.last, window) {
			violation(check, "writer %d has %d rows from %d to %d", w, r.count, r.first, r.last)
		}
		result[w] = r.last
	}
	return result
}

// keepWalSmall checkpoints the local WAL, kept in memory here, which otherwise grows for good.
func keepWalSmall(c *client) error {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	stats, err := c.sync.Stats(ctx)
	if err != nil {
		return err
	}
	if stats.MainWalSize <= clientWalBytes {
		return nil
	}
	return timed("client_checkpoint", c.sync.Checkpoint)
}

// Workers.

type writer struct {
	w         int64
	client    *client
	attempted atomic.Int64
	acked     atomic.Int64
}

var (
	writers []*writer
	readers []*client
	// Held for writing while a comparison needs every client idle.
	gate sync.RWMutex
)

func acked() map[int64]int64 {
	floor := map[int64]int64{}
	for _, wr := range writers {
		floor[wr.w] = wr.acked.Load()
	}
	return floor
}

// checkBounds checks that rows acknowledged before floor was taken are
// there, and rows never written are not.
func checkBounds(check string, held, floor map[int64]int64) {
	for _, wr := range writers {
		top := held[wr.w]
		if top < floor[wr.w] {
			violation(check, "writer %d ends at %d, the server acknowledged %d", wr.w, top, floor[wr.w])
		}
		if attempted := wr.attempted.Load(); top > attempted {
			violation(check, "writer %d ends at %d, only %d were written", wr.w, top, attempted)
		}
	}
}

func startWriter(w int64) (*writer, error) {
	ctx := context.Background()
	c, err := connect(ctx, fmt.Sprintf("go-writer-%d", w), 0)
	if err != nil {
		return nil, err
	}
	base := w * idSpan
	var top sql.NullInt64
	if err := c.db.QueryRowContext(ctx, "SELECT max(x) FROM t WHERE x > ? AND x < ?", base, base+idSpan).Scan(&top); err != nil {
		c.close()
		return nil, err
	}
	wr := &writer{w: w, client: c}
	if top.Valid {
		wr.attempted.Store(top.Int64 - base)
		wr.acked.Store(top.Int64 - base)
	}
	set("soak_rows_acked", labels{{"writer", strconv.FormatInt(w, 10)}}, float64(wr.acked.Load()))
	return wr, nil
}

func (wr *writer) push(op string) error {
	target := wr.attempted.Load()
	if err := timed(op, wr.client.sync.Push); err != nil {
		return err
	}
	wr.acked.Store(target)
	set("soak_rows_acked", labels{{"writer", strconv.FormatInt(wr.w, 10)}}, float64(target))
	return nil
}

func (wr *writer) write(rows int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	base := wr.w * idSpan
	first := wr.attempted.Load() + 1
	last := first + rows - 1
	tuples := make([]string, rows)
	args := make([]any, 0, 2*rows)
	for i := range rows {
		tuples[i] = "(?, hex(randomblob(?)))"
		args = append(args, base+first+i, rowBytes/2)
	}
	tx, err := wr.client.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES "+strings.Join(tuples, ", "), args...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM t WHERE x > ? AND x <= ?", base, base+last-window); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	wr.attempted.Store(last)
	return nil
}

func runWriter(wr *writer) {
	nextBurst := time.Now().Add(jitter(burstInterval))
	pause := func() time.Duration { return jitter(writeInterval) }
	forever(wr.client.name, pause, pause(), func() error {
		gate.RLock()
		defer gate.RUnlock()
		op, rows := "push", int64(1)
		if time.Now().After(nextBurst) {
			nextBurst = time.Now().Add(jitter(burstInterval))
			op, rows = "burst", burstRows
		}
		if err := wr.write(rows); err != nil {
			return err
		}
		// A failed push leaves the rows pending; the next one sends them.
		if err := wr.push(op); err != nil {
			return err
		}
		return keepWalSmall(wr.client)
	})
}

func runReader(index int) {
	c := readers[index]
	seen := map[int64]int64{}
	pulls, failures := 0, 0
	pause := func() time.Duration { return jitter(pullInterval) }
	forever(c.name, pause, pause(), func() error {
		gate.RLock()
		defer gate.RUnlock()
		if err := timed("pull", c.pull); err != nil {
			if failures++; failures < 10 {
				return err
			}
			failures = 0
			var fresh *client
			if err := timed("reconnect", func(ctx context.Context) (err error) {
				fresh, err = connect(ctx, c.name, 0)
				return err
			}); err != nil {
				return err
			}
			c.close()
			c, readers[index] = fresh, fresh
		} else {
			failures = 0
		}
		if err := keepWalSmall(c); err != nil {
			return err
		}
		if pulls++; pulls%10 != 0 {
			return nil
		}
		xs, err := values(context.Background(), c.db)
		if err != nil {
			return err
		}
		held := tops("reader", xs)
		for w, top := range held {
			if top < seen[w] {
				violation("reader", "%s: writer %d went back from %d to %d", c.name, w, seen[w], top)
			}
			seen[w] = max(top, seen[w])
		}
		checkBounds("reader", held, map[int64]int64{})
		return nil
	})
}

func runBootstraps() {
	count := 0
	pause := func() time.Duration { return jitter(bootstrapEvery) }
	forever("bootstrap", pause, pause(), func() error {
		floor := acked()
		// A chunked download pins a revision, holding off checkpoints for a while.
		count++
		op, pullBytes := "bootstrap", 0
		if chunkedEvery > 0 && count%chunkedEvery == 0 {
			op, pullBytes = "bootstrap_chunked", 256*1024
		}
		var c *client
		if err := timed(op, func(ctx context.Context) (err error) {
			c, err = connect(ctx, "go-bootstrap", pullBytes)
			return err
		}); err != nil {
			return err
		}
		defer c.close()
		xs, err := values(context.Background(), c.db)
		if err != nil {
			return err
		}
		checkBounds("bootstrap", tops("bootstrap", xs), floor)
		return nil
	})
}

// compare checks that, with writes paused and pushed, every client pulls to
// the same state as a fresh one.
func compare(ctx context.Context) error {
	gate.Lock()
	defer gate.Unlock()
	for _, wr := range writers {
		if wr.acked.Load() < wr.attempted.Load() {
			if err := wr.push("push"); err != nil {
				return err
			}
		}
	}
	fresh, err := connect(ctx, "go-compare", 0)
	if err != nil {
		return err
	}
	defer fresh.close()
	xs, err := values(ctx, fresh.db)
	if err != nil {
		return err
	}
	held := tops("compare", xs)
	for _, wr := range writers {
		if attempted := wr.attempted.Load(); held[wr.w] != attempted {
			violation("compare", "writer %d ends at %d, wrote %d", wr.w, held[wr.w], attempted)
		}
	}
	expected, err := digest(ctx, fresh)
	if err != nil {
		return err
	}
	clients := slices.Clone(readers)
	for _, wr := range writers {
		clients = append(clients, wr.client)
	}
	for _, c := range clients {
		if err := c.pull(ctx); err != nil {
			return err
		}
		got, err := digest(ctx, c)
		if err != nil {
			return err
		}
		if got != expected {
			violation("compare", "%s differs from a fresh client", c.name)
		}
	}
	set("soak_last_ok_timestamp_seconds", labels{{"check", "compare"}}, float64(time.Now().Unix()))
	return nil
}

// restore restores the backup next to the running server and checks it holds the acknowledged writes.
func restore(ctx context.Context) error {
	floor := acked()
	time.Sleep(backupLag)
	dir, err := os.MkdirTemp("", "soak-restore-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "restored.db")
	output, err := exec.CommandContext(ctx, "turso-sync-server", "restore", "--backup-url", backupURL, path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	db, err := sql.Open("turso", path)
	if err != nil {
		return err
	}
	defer db.Close()
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		return err
	}
	if integrity != "ok" {
		violation("restore", "integrity check: %s", integrity)
	}
	xs, err := values(ctx, db)
	if err != nil {
		return err
	}
	checkBounds("restore", tops("restore", xs), floor)
	set("soak_last_ok_timestamp_seconds", labels{{"check", "restore"}}, float64(time.Now().Unix()))
	return nil
}

func serverStatus() string {
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(serverMetrics)
	if err != nil {
		return "server metrics unavailable: " + err.Error()
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "server metrics unavailable: " + err.Error()
	}
	lines := strings.Split(string(body), "\n")
	sum := func(name, match string) float64 {
		total := 0.0
		for _, line := range lines {
			if (strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ")) && strings.Contains(line, match) {
				fields := strings.Fields(line)
				value, _ := strconv.ParseFloat(fields[len(fields)-1], 64)
				total += value
			}
		}
		return total
	}
	lag := float64(time.Now().UnixMilli())/1000 - sum("turso_sync_backup_synced_timestamp_seconds", "")
	return fmt.Sprintf("server rss %.0f MiB, fds %.0f, wal %.0f frames, generation %.0f, backup lag %.1f s, uploads %.0f ok %.0f failed",
		sum("process_resident_memory_bytes", "")/(1<<20), sum("process_open_fds", ""),
		sum("turso_sync_wal_frames", ""), sum("turso_sync_generation", ""), lag,
		sum("turso_sync_backup_uploads_total", `result="ok"`), sum("turso_sync_backup_uploads_total", `result="error"`))
}

func status() error {
	var ops []string
	for _, op := range []string{"push", "burst", "pull", "client_checkpoint", "bootstrap", "bootstrap_chunked", "compare", "restore"} {
		ops = append(ops, fmt.Sprintf("%s %.0f/%.0f", op,
			total("soak_operations_total", fmt.Sprintf(`op="%s",result="ok"`, op)),
			total("soak_operations_total", fmt.Sprintf(`op="%s",result="error"`, op))))
	}
	rows := int64(0)
	for _, wr := range writers {
		rows += wr.acked.Load()
	}
	log.Printf("rows %d, ok/failed: %s, violations %.0f, rss %.0f MiB; %s", rows, strings.Join(ops, ", "),
		total("soak_violations_total", ""), residentBytes()/(1<<20), serverStatus())
	return nil
}

func main() {
	log.SetFlags(log.LUTC | log.Ldate | log.Ltime | log.Lmicroseconds)
	go func() {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = w.Write([]byte(exposition()))
		})
		log.Fatal(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", metricsPort), nil))
	}()

	backup := backupURL
	if backup == "" {
		backup = "not checked"
	}
	log.Printf("soak (%s, tursogo) on %s: %d writers from %d, %d readers, backup %s",
		runtime.Version(), syncURL, writerCount, firstWriter, readerCount, backup)
	retry("init", func() (struct{}, error) {
		ctx := context.Background()
		c, err := connect(ctx, "go-init", 0)
		if err != nil {
			return struct{}{}, err
		}
		defer c.close()
		if _, err := c.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS t(x INTEGER PRIMARY KEY, y TEXT)"); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, c.sync.Push(ctx)
	})
	for i := range int64(writerCount) {
		writers = append(writers, retry("init", func() (*writer, error) { return startWriter(firstWriter + i) }))
	}
	for i := 1; i <= readerCount; i++ {
		readers = append(readers, retry("init", func() (*client, error) {
			return connect(context.Background(), fmt.Sprintf("go-reader-%d", i), 0)
		}))
	}
	var started []string
	for _, wr := range writers {
		started = append(started, strconv.FormatInt(wr.acked.Load(), 10))
	}
	log.Printf("started from %s rows per writer", strings.Join(started, ", "))

	for _, wr := range writers {
		go runWriter(wr)
	}
	for i := range readers {
		go runReader(i)
	}
	go runBootstraps()
	go forever("compare", func() time.Duration { return compareInterval }, min(compareInterval, 5*time.Minute),
		func() error { return timed("compare", compare) })
	if backupURL != "" {
		go forever("restore", func() time.Duration { return restoreInterval }, firstRestore,
			func() error { return timed("restore", restore) })
	}
	forever("status", func() time.Duration { return statusInterval }, statusInterval, status)
}
