package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"syscall"
	"testing"
	"time"
)

func TestRetryBusyUnmount(t *testing.T) {
	attempts := 0
	err := retryBusyUnmount(context.Background(), discardLogger(), time.Nanosecond, func() error {
		attempts++
		if attempts < 3 {
			return syscall.EBUSY
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("unmount attempts = %d, want 3", attempts)
	}
}

func TestRetryBusyUnmountReturnsOtherErrors(t *testing.T) {
	want := errors.New("unmount failed")
	attempts := 0
	err := retryBusyUnmount(context.Background(), discardLogger(), time.Nanosecond, func() error {
		attempts++
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("unmount error = %v, want %v", err, want)
	}
	if attempts != 1 {
		t.Fatalf("unmount attempts = %d, want 1", attempts)
	}
}

func TestRetryBusyUnmountHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := retryBusyUnmount(ctx, discardLogger(), time.Hour, func() error {
		return syscall.EBUSY
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("unmount error = %v, want context cancellation joined with EBUSY", err)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
