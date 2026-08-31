//go:build linux

package block

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"

	backendpkg "github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/replica"
)

func TestExt4MountRoundTrip(t *testing.T) {
	if os.Getenv("SATCHEL_NBD_TEST") != "1" {
		t.Skip("set SATCHEL_NBD_TEST=1 to exercise a real NBD mount")
	}
	dir := t.TempDir()
	image := filepath.Join(dir, "image")
	file, err := os.OpenFile(image, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(64 << 20); err != nil {
		t.Fatal(err)
	}
	file.Close()
	backend := New(Options{})
	if err := backend.Format(context.Background(), image, "ext4"); err != nil {
		t.Fatal(err)
	}
	device, err := replica.OpenDevice(image, 64<<20, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	var flushes atomic.Int64
	device.SetFlushHandler(func() error {
		flushes.Add(1)
		return nil
	})
	mountpoint := filepath.Join(dir, "mnt")
	mount, err := backend.Mount(context.Background(), device, mountpoint, backendpkg.MountOptions{Filesystem: "ext4"})
	if err != nil {
		device.Close()
		t.Fatal(err)
	}
	dataFile, err := os.OpenFile(filepath.Join(mountpoint, "hello"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dataFile.Write([]byte("block storage")); err != nil {
		dataFile.Close()
		t.Fatal(err)
	}
	if err := dataFile.Sync(); err != nil {
		dataFile.Close()
		t.Fatal(err)
	}
	if err := dataFile.Close(); err != nil {
		t.Fatal(err)
	}
	if flushes.Load() == 0 {
		t.Fatal("fsync did not reach the NBD flush handler")
	}
	if err := mount.Unmount(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := device.Close(); err != nil {
		t.Fatal(err)
	}

	device, err = replica.OpenDevice(image, 64<<20, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	mount, err = backend.Mount(context.Background(), device, mountpoint, backendpkg.MountOptions{ReadOnly: true, Filesystem: "ext4"})
	if err != nil {
		device.Close()
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(mountpoint, "hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "block storage" {
		t.Fatalf("read %q", data)
	}
	if err := mount.Unmount(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := device.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNBDReadWriteProtocol(t *testing.T) {
	device, err := replica.OpenDevice(filepath.Join(t.TempDir(), "image"), 8*replica.DefaultBlockSize, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, false) }()

	payload := bytes.Repeat([]byte("w"), replica.DefaultBlockSize)
	writeRequest(t, client, nbdCmdWrite, 17, replica.DefaultBlockSize, uint32(len(payload)), payload)
	readReply(t, client, 17, nil)
	writeRequest(t, client, nbdCmdRead, 18, replica.DefaultBlockSize, uint32(len(payload)), nil)
	readReply(t, client, 18, payload)
	writeRequest(t, client, nbdCmdDisconnect, 19, 0, 0, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNBDRejectsWritesWhenReadOnly(t *testing.T) {
	device, err := replica.OpenDevice(filepath.Join(t.TempDir(), "image"), 8*replica.DefaultBlockSize, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, true) }()
	writeRequest(t, client, nbdCmdWrite, 1, 0, replica.DefaultBlockSize, make([]byte, replica.DefaultBlockSize))
	reply := make([]byte, 16)
	if _, err := io.ReadFull(client, reply); err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint32(reply[4:8]); got == 0 {
		t.Fatal("read-only write succeeded")
	}
	writeRequest(t, client, nbdCmdDisconnect, 2, 0, 0, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNBDFlushAndFUAReachDeviceFlushHandler(t *testing.T) {
	device, err := replica.OpenDevice(filepath.Join(t.TempDir(), "image"), 8*replica.DefaultBlockSize, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	var flushes atomic.Int64
	device.SetFlushHandler(func() error {
		flushes.Add(1)
		return nil
	})
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, false) }()

	writeRequest(t, client, nbdCmdFlush, 1, 0, 0, nil)
	readReply(t, client, 1, nil)
	payload := bytes.Repeat([]byte("w"), replica.DefaultBlockSize)
	writeRequest(t, client, nbdCmdWrite|nbdCmdFlagFUA, 2, 0, uint32(len(payload)), payload)
	readReply(t, client, 2, nil)
	if got := flushes.Load(); got != 2 {
		t.Fatalf("flush handler calls = %d, want 2", got)
	}

	writeRequest(t, client, nbdCmdDisconnect, 3, 0, 0, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNBDReportsFlushFailure(t *testing.T) {
	device, err := replica.OpenDevice(filepath.Join(t.TempDir(), "image"), 8*replica.DefaultBlockSize, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	device.SetFlushHandler(func() error { return errors.New("remote unavailable") })
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, false) }()

	writeRequest(t, client, nbdCmdFlush, 1, 0, 0, nil)
	reply := make([]byte, 16)
	if _, err := io.ReadFull(client, reply); err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint32(reply[4:8]); got != uint32(unix.EIO) {
		t.Fatalf("flush errno = %d, want %d", got, unix.EIO)
	}

	writeRequest(t, client, nbdCmdDisconnect, 2, 0, 0, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func writeRequest(t *testing.T, conn net.Conn, command uint32, handle, offset uint64, length uint32, payload []byte) {
	t.Helper()
	header := make([]byte, 28)
	binary.BigEndian.PutUint32(header[0:4], nbdRequestMagic)
	binary.BigEndian.PutUint32(header[4:8], command)
	binary.BigEndian.PutUint64(header[8:16], handle)
	binary.BigEndian.PutUint64(header[16:24], offset)
	binary.BigEndian.PutUint32(header[24:28], length)
	if _, err := conn.Write(append(header, payload...)); err != nil {
		t.Fatal(err)
	}
}

func readReply(t *testing.T, conn net.Conn, handle uint64, payload []byte) {
	t.Helper()
	reply := make([]byte, 16+len(payload))
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint32(reply[:4]); got != nbdReplyMagic {
		t.Fatalf("reply magic = %#x", got)
	}
	if got := binary.BigEndian.Uint32(reply[4:8]); got != 0 {
		t.Fatalf("reply errno = %d", got)
	}
	if got := binary.BigEndian.Uint64(reply[8:16]); got != handle {
		t.Fatalf("reply handle = %d, want %d", got, handle)
	}
	if !bytes.Equal(reply[16:], payload) {
		t.Fatal("reply data differs")
	}
}
