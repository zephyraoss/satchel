package store

import (
	"context"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"testing"
)

func benchDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open(context.Background(), filepath.Join(b.TempDir(), "b.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func BenchmarkCreateAndWrite4K(b *testing.B) {
	db := benchDB(b)
	ctx := context.Background()
	payload := make([]byte, 4096)
	rand.Read(payload)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("f%d", i)
		err := db.Do(ctx, func(tx *Tx) error {
			attr, err := tx.Create(RootIno, name, NewInode{Mode: regMode | 0o644})
			if err != nil {
				return err
			}
			_, err = tx.WriteAt(attr.Ino, payload, 0)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEmptyTx(b *testing.B) {
	db := benchDB(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Do(ctx, func(tx *Tx) error { return nil })
	}
}

func BenchmarkStatTx(b *testing.B) {
	db := benchDB(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.View(ctx, func(tx *Tx) error { _, err := tx.Stat(RootIno); return err })
	}
}

func BenchmarkRandomWrite4K(b *testing.B) {
	db := benchDB(b)
	ctx := context.Background()
	var ino uint64
	db.Do(ctx, func(tx *Tx) error {
		attr, err := tx.Create(RootIno, "r", NewInode{Mode: regMode | 0o644})
		ino = attr.Ino
		return err
	})
	const size = 64 << 20
	full := make([]byte, DefaultChunkSize)
	rand.Read(full)
	db.Do(ctx, func(tx *Tx) error {
		for off := int64(0); off < size; off += DefaultChunkSize {
			if _, err := tx.WriteAt(ino, full, off); err != nil {
				return err
			}
		}
		return nil
	})
	block := make([]byte, 4096)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(i*7919%(size/4096)) * 4096
		err := db.Do(ctx, func(tx *Tx) error {
			_, err := tx.WriteAt(ino, block, off)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
