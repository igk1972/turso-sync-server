package synctest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	turso "turso.tech/database/tursogo"
)

var binary = func() string {
	if path := os.Getenv("LOCAL_SYNC_SERVER"); path != "" {
		return path
	}
	path, err := filepath.Abs("../../target/release/turso-sync-server")
	if err != nil {
		panic(err)
	}
	return path
}()

var noTimer = []string{"--checkpoint-interval", "0"}

type server struct {
	url  string
	port int
	cmd  *exec.Cmd
	done chan struct{}
}

func freePort(t *testing.T) int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// startServer runs the server with args on port, or on a free port when it is 0.
func startServer(t *testing.T, port int, args ...string) *server {
	t.Helper()
	if port == 0 {
		port = freePort(t)
	}
	cmd := exec.Command(binary, append(args, "--sync-server", fmt.Sprintf("127.0.0.1:%d", port))...)
	// Settings meant for the upstream suite must not leak into these servers.
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "TURSO_SYNC_") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	if os.Getenv("SYNC_SERVER_LOGS") != "" {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	}
	require.NoError(t, cmd.Start())
	s := &server{url: fmt.Sprintf("http://127.0.0.1:%d", port), port: port, cmd: cmd, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(s.done)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-s.done
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-s.done:
			t.Fatalf("server exited: %v", cmd.ProcessState)
		default:
		}
		resp, err := http.Get(s.url + "/health")
		if err == nil {
			resp.Body.Close()
			return s
		}
		require.True(t, time.Now().Before(deadline), "server did not become ready")
		time.Sleep(50 * time.Millisecond)
	}
}

// stop shuts the server down gracefully, which uploads the rest of a backup.
func (s *server) stop(t *testing.T) {
	t.Helper()
	require.NoError(t, s.cmd.Process.Signal(syscall.SIGTERM))
	<-s.done
}

// sql runs a statement on the server directly, bypassing any client.
func (s *server) sql(t *testing.T, statement string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"requests": []map[string]any{{"type": "execute", "stmt": map[string]any{"sql": statement}}},
	})
	require.NoError(t, err)
	resp, err := http.Post(s.url+"/v2/pipeline", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		Results []struct {
			Type string `json:"type"`
		} `json:"results"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Equal(t, "ok", result.Results[0].Type, statement)
}

func (s *server) checkpoint(t *testing.T, path string) map[string]any {
	t.Helper()
	resp, err := http.Post(s.url+path+"/admin/checkpoint", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var outcome map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&outcome))
	return outcome
}

type client struct {
	db   *turso.TursoSyncDb
	conn *sql.DB
}

func connect(t *testing.T, config turso.TursoSyncDbConfig) (*client, error) {
	t.Helper()
	if config.Path == "" {
		config.Path = ":memory:"
	}
	db, err := turso.NewTursoSyncDb(context.Background(), config)
	if err != nil {
		return nil, err
	}
	conn, err := db.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { conn.Close() })
	return &client{db: db, conn: conn}, nil
}

func mustConnect(t *testing.T, url string) *client {
	t.Helper()
	c, err := connect(t, turso.TursoSyncDbConfig{RemoteUrl: url})
	require.NoError(t, err)
	return c
}

func (c *client) exec(t *testing.T, sql string) {
	t.Helper()
	_, err := c.conn.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

// insert pushes rows large enough to sit on pages of their own: a lost page
// then loses a row instead of being masked by a later copy of a shared page.
func (c *client) insert(t *testing.T, values ...int) {
	t.Helper()
	tuples := make([]string, len(values))
	for i, v := range values {
		tuples[i] = fmt.Sprintf("(%d, randomblob(3000))", v)
	}
	c.exec(t, "INSERT INTO t VALUES "+strings.Join(tuples, ", "))
	require.NoError(t, c.db.Push(context.Background()))
}

func (c *client) pull(t *testing.T) {
	t.Helper()
	_, err := c.db.Pull(context.Background())
	require.NoError(t, err)
}

func (c *client) values(t *testing.T) []int {
	t.Helper()
	rows, err := c.conn.QueryContext(context.Background(), "SELECT x FROM t ORDER BY x")
	require.NoError(t, err)
	defer rows.Close()
	values := []int{}
	for rows.Next() {
		var v int
		require.NoError(t, rows.Scan(&v))
		values = append(values, v)
	}
	require.NoError(t, rows.Err())
	return values
}

func (c *client) count(t *testing.T, sql string) int {
	t.Helper()
	var n int
	require.NoError(t, c.conn.QueryRowContext(context.Background(), sql).Scan(&n))
	return n
}

// syncedPair creates table t with row 1 through a writer and bootstraps a reader.
func syncedPair(t *testing.T, url string) (writer, reader *client) {
	t.Helper()
	writer = mustConnect(t, url)
	writer.exec(t, "CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)")
	writer.insert(t, 1)
	reader = mustConnect(t, url)
	require.Equal(t, []int{1}, reader.values(t))
	return writer, reader
}

func valuesUpTo(n int) []int {
	values := make([]int, n)
	for i := range values {
		values[i] = i + 1
	}
	return values
}
