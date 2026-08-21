package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestLiveWALBytesTracksUncheckpointedFrames(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "w.db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `PRAGMA wal_autocheckpoint = 0`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	before := LiveWALBytes(path)
	f := createFile(t, db, RootIno, "f")
	payload := make([]byte, 1<<20)
	if err := db.Do(ctx, func(tx *Tx) error {
		_, err := tx.WriteAt(f.Ino, payload, 0)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after := LiveWALBytes(path)
	if after-before < 1<<20 {
		t.Fatalf("live WAL grew by %d bytes after a 1 MiB write", after-before)
	}
	if pending := PendingWALBytes(path); pending < 1<<20 {
		t.Fatalf("pending WAL = %d before checkpoint", pending)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		t.Fatal(err)
	}
	if pending := PendingWALBytes(path); pending != 0 {
		t.Fatalf("pending WAL = %d after passive checkpoint", pending)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if got := LiveWALBytes(path); got > before+4096 {
		t.Fatalf("live WAL = %d after truncate checkpoint", got)
	}
}
