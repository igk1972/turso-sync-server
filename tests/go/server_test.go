package synctest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	turso "turso.tech/database/tursogo"
)

func TestAuthToken(t *testing.T) {
	s := startServer(t, 0, ":memory:", "--auth-token", "secret")

	_, err := connect(t, turso.TursoSyncDbConfig{RemoteUrl: s.url, AuthToken: "wrong"})
	require.Error(t, err)
	_, err = connect(t, turso.TursoSyncDbConfig{RemoteUrl: s.url})
	require.Error(t, err)

	writer, err := connect(t, turso.TursoSyncDbConfig{RemoteUrl: s.url, AuthToken: "secret"})
	require.NoError(t, err)
	writer.exec(t, "CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)")
	writer.insert(t, 1)
	reader, err := connect(t, turso.TursoSyncDbConfig{RemoteUrl: s.url, AuthToken: "secret"})
	require.NoError(t, err)
	require.Equal(t, []int{1}, reader.values(t))
}

func TestClientsCatchUpAcrossCheckpoints(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "main.db")
	s := startServer(t, 0, append([]string{dbPath}, noTimer...)...)
	writer, reader := syncedPair(t, s.url)

	for x := 2; x <= 4; x++ {
		writer.insert(t, x)
		require.Equal(t, "done", s.checkpoint(t, "")["status"])
	}
	info, err := os.Stat(dbPath + "-wal")
	require.NoError(t, err)
	require.Zero(t, info.Size())

	reader.pull(t)
	require.Equal(t, valuesUpTo(4), reader.values(t))
	require.Equal(t, valuesUpTo(4), mustConnect(t, s.url).values(t))
}

func TestChunkedBootstrapHoldsCheckpointsOff(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "main.db")
	s := startServer(t, 0, append([]string{dbPath}, noTimer...)...)
	seed := mustConnect(t, s.url)
	seed.exec(t, "CREATE TABLE big(x INTEGER PRIMARY KEY, y BLOB)")
	seed.exec(t, "INSERT INTO big SELECT value, randomblob(1024) FROM generate_series(1, 50)")
	require.NoError(t, seed.db.Push(t.Context()))
	require.Equal(t, "done", s.checkpoint(t, "")["status"])
	seed.exec(t, "INSERT INTO big SELECT value, randomblob(1024) FROM generate_series(51, 60)")
	require.NoError(t, seed.db.Push(t.Context()))

	chunked, err := connect(t, turso.TursoSyncDbConfig{RemoteUrl: s.url, PullBytesThreshold: 8192})
	require.NoError(t, err)
	require.Equal(t, 60, chunked.count(t, "SELECT COUNT(*) FROM big"))
	require.Equal(t, 60*1024, chunked.count(t, "SELECT SUM(length(y)) FROM big"))
	require.Equal(t, "deferred", s.checkpoint(t, "")["status"])
}

func TestDatabasesUnderSyncDir(t *testing.T) {
	dir := t.TempDir()
	s := startServer(t, 0, append([]string{"--sync-dir", dir}, noTimer...)...)
	alphaWriter, alphaReader := syncedPair(t, s.url+"/db/alpha")
	_, betaReader := syncedPair(t, s.url+"/db/beta")

	alphaWriter.insert(t, 2)
	require.Equal(t, "done", s.checkpoint(t, "/db/alpha")["status"])
	alphaReader.pull(t)
	betaReader.pull(t)
	require.Equal(t, []int{1, 2}, alphaReader.values(t))
	require.Equal(t, []int{1}, betaReader.values(t))
}

func TestRestoreFromBackup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "main.db")
	args := append([]string{dbPath, "--backup-url", filepath.Join(dir, "backup")}, noTimer...)
	s := startServer(t, 0, args...)
	writer, reader := syncedPair(t, s.url)
	writer.insert(t, 2)
	require.Equal(t, "done", s.checkpoint(t, "")["status"])
	writer.insert(t, 3)
	s.stop(t)

	for _, suffix := range []string{"", "-wal", "-shm", "-sync"} {
		require.NoError(t, os.RemoveAll(dbPath+suffix))
	}
	s = startServer(t, s.port, args...)
	require.Equal(t, valuesUpTo(3), mustConnect(t, s.url).values(t))
	reader.pull(t)
	require.Equal(t, valuesUpTo(3), reader.values(t))
	writer.insert(t, 4)
	reader.pull(t)
	require.Equal(t, valuesUpTo(4), reader.values(t))
}
