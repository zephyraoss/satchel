package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWALStateTracksUncheckpointedFrames(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "w.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	before := db.WALState().LiveBytes()
	f := createFile(t, db, RootIno, "f")
	payload := make([]byte, 1<<20)
	if err := db.Do(ctx, func(tx *Tx) error {
		_, err := tx.WriteAt(f.Ino, payload, 0)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after := db.WALState().LiveBytes()
	if after-before < 1<<20 {
		t.Fatalf("live WAL grew by %d bytes after a 1 MiB write", after-before)
	}
	if pending := db.WALState().PendingBytes(); pending < 1<<20 {
		t.Fatalf("pending WAL = %d before checkpoint", pending)
	}
	if outOfProcess := ReadWALState(path).PendingBytes(); outOfProcess != db.WALState().PendingBytes() {
		t.Fatalf("ReadWALState = %d, db.WALState = %d", outOfProcess, db.WALState().PendingBytes())
	}
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		t.Fatal(err)
	}
	if pending := db.WALState().PendingBytes(); pending != 0 {
		t.Fatalf("pending WAL = %d after passive checkpoint", pending)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if got := db.WALState().LiveBytes(); got > before+4096 {
		t.Fatalf("live WAL = %d after truncate checkpoint", got)
	}
}

const writeLockProbeEnv = "SATCHEL_TEST_WRITE_LOCK_PROBE"

func TestWALStateKeepsWriteLockAgainstOtherProcesses(t *testing.T) {
	if dbPath := os.Getenv(writeLockProbeEnv); dbPath != "" {
		probeWriteLock(dbPath)
		return
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "w.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Do(ctx, func(tx *Tx) error {
		if _, err := tx.Create(RootIno, "held", NewInode{Mode: regMode | 0o644}); err != nil {
			return err
		}
		for i := 0; i < 50; i++ {
			db.WALState()
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestWALStateKeepsWriteLockAgainstOtherProcesses$", "-test.v")
		cmd.Env = append(os.Environ(), writeLockProbeEnv+"="+path)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("probe process: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "probe: busy") {
			return fmt.Errorf("another process acquired the write lock while this one held a transaction:\n%s", out)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func probeWriteLock(dbPath string) {
	raw, err := sql.Open(driverName, "file:"+dbPath)
	if err != nil {
		fmt.Println("probe: open:", err)
		os.Exit(1)
	}
	defer raw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := raw.Conn(ctx)
	if err != nil {
		fmt.Println("probe: conn:", err)
		os.Exit(1)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout = 0`); err != nil {
		fmt.Println("probe: busy_timeout:", err)
		os.Exit(1)
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		if isBusy(err) {
			fmt.Println("probe: busy")
			return
		}
		fmt.Println("probe: begin:", err)
		os.Exit(1)
	}
	conn.ExecContext(ctx, `ROLLBACK`)
	fmt.Println("probe: acquired")
}
