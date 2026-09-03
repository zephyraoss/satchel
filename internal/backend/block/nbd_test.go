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
	"time"

	"golang.org/x/sys/unix"

	backendpkg "github.com/zephyraoss/satchel/internal/backend"
	"github.com/zephyraoss/satchel/internal/objectstore"
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

func TestExt4LazyRestoreRoundTrip(t *testing.T) {
	if os.Getenv("SATCHEL_NBD_TEST") != "1" {
		t.Skip("set SATCHEL_NBD_TEST=1 to exercise a real NBD mount")
	}
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	backend := New(Options{})
	file, err := os.OpenFile(source, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(64 << 20); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err := backend.Format(ctx, source, "ext4"); err != nil {
		t.Fatal(err)
	}
	device, err := replica.OpenDevice(source, 64<<20, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := device.MarkAllocated(); err != nil {
		device.Close()
		t.Fatal(err)
	}
	mountpoint := filepath.Join(dir, "source-mnt")
	mount, err := backend.Mount(ctx, device, mountpoint, backendpkg.MountOptions{Filesystem: "ext4"})
	if err != nil {
		device.Close()
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("lazy-block-storage"), 4096)
	if err := os.WriteFile(filepath.Join(mountpoint, "database"), want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := mount.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	generation := device.Seal()
	segments, err := replica.EncodeSegments(generation, replica.DefaultSegmentBlocks)
	if err != nil {
		t.Fatal(err)
	}
	store := objectstore.NewMemory()
	remote := &replica.Remote{Store: store}
	lease, _, err := remote.Acquire(ctx, "data", "node-a", replica.CreateOptions{Size: 64 << 20, Filesystem: "ext4"})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Publish(ctx, segments...); err != nil {
		t.Fatal(err)
	}
	device.Release(generation)
	if err := device.Close(); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(dir, "target")
	lazy, err := remote.PrepareLazyRestore(ctx, lease.State(), target)
	if err != nil {
		t.Fatal(err)
	}
	device, err = replica.OpenDevice(target, 64<<20, 64<<20)
	if err != nil {
		lazy.Close()
		t.Fatal(err)
	}
	device.SetLazyImage(lazy)
	targetMount := filepath.Join(dir, "target-mnt")
	mount, err = backend.Mount(ctx, device, targetMount, backendpkg.MountOptions{ReadOnly: true, Filesystem: "ext4"})
	if err != nil {
		device.Close()
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(targetMount, "database"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("lazy ext4 restore returned different data")
	}
	if err := mount.Unmount(ctx); err != nil {
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

func TestNBDFlushWaitsForPriorWrites(t *testing.T) {
	device, err := replica.OpenDevice(filepath.Join(t.TempDir(), "image"), 4*replica.DefaultBlockSize, 4*replica.DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	payload := bytes.Repeat([]byte("w"), replica.DefaultBlockSize)
	device.SetRemoteFlushHandler(func(generation *replica.Generation) <-chan error {
		if got := generation.Blocks[1]; !bytes.Equal(got, payload) {
			return completedFlush(errors.New("flush generation does not contain the prior write"))
		}
		return completedFlush(nil)
	})
	client, server := net.Pipe()
	setNBDTestDeadline(t, client)
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, false) }()

	writeRequest(t, client, nbdCmdWrite, 1, replica.DefaultBlockSize, uint32(len(payload)), payload)
	readReply(t, client, 1, nil)
	writeRequest(t, client, nbdCmdFlush, 2, 0, 0, nil)
	readReply(t, client, 2, nil)
	writeRequest(t, client, nbdCmdDisconnect, 3, 0, 0, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNBDAllowsWritesDuringRemoteFlush(t *testing.T) {
	device, err := replica.OpenDevice(filepath.Join(t.TempDir(), "image"), 4*replica.DefaultBlockSize, 4*replica.DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	device.SetRemoteFlushHandler(func(*replica.Generation) <-chan error {
		result := make(chan error, 1)
		close(flushStarted)
		go func() {
			<-releaseFlush
			result <- nil
		}()
		return result
	})
	client, server := net.Pipe()
	setNBDTestDeadline(t, client)
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, false) }()

	first := bytes.Repeat([]byte("a"), replica.DefaultBlockSize)
	writeRequest(t, client, nbdCmdWrite, 1, 0, uint32(len(first)), first)
	readReply(t, client, 1, nil)
	writeRequest(t, client, nbdCmdFlush, 2, 0, 0, nil)
	select {
	case <-flushStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("remote flush did not start")
	}
	second := bytes.Repeat([]byte("b"), replica.DefaultBlockSize)
	writeRequest(t, client, nbdCmdWrite, 3, replica.DefaultBlockSize, uint32(len(second)), second)
	readReply(t, client, 3, nil)
	close(releaseFlush)
	readReply(t, client, 2, nil)
	writeRequest(t, client, nbdCmdDisconnect, 4, 0, 0, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNBDKeepsFlushBarriersOrdered(t *testing.T) {
	device, err := replica.OpenDevice(filepath.Join(t.TempDir(), "image"), 4*replica.DefaultBlockSize, 4*replica.DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	queued := make(chan int, 2)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var calls atomic.Int64
	device.SetRemoteFlushHandler(func(*replica.Generation) <-chan error {
		call := int(calls.Add(1))
		result := make(chan error, 1)
		queued <- call
		go func() {
			<-releases[call-1]
			result <- nil
		}()
		return result
	})
	client, server := net.Pipe()
	setNBDTestDeadline(t, client)
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, false) }()

	writeRequest(t, client, nbdCmdFlush, 1, 0, 0, nil)
	writeRequest(t, client, nbdCmdFlush, 2, 0, 0, nil)
	for want := 1; want <= 2; want++ {
		if got := <-queued; got != want {
			t.Fatalf("queued flush = %d, want %d", got, want)
		}
	}
	close(releases[1])
	if err := client.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(client, make([]byte, 16)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("later flush replied before the earlier barrier completed: %v", err)
	}
	if err := client.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	close(releases[0])
	for range 2 {
		reply := readNBDReply(t, client, 0)
		if reply.handle != 1 && reply.handle != 2 {
			t.Fatalf("unexpected flush reply handle %d", reply.handle)
		}
	}
	writeRequest(t, client, nbdCmdDisconnect, 3, 0, 0, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNBDFUAFlushesItsWrite(t *testing.T) {
	image := filepath.Join(t.TempDir(), "image")
	device, err := replica.OpenDevice(image, 4*replica.DefaultBlockSize, 4*replica.DefaultBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	payload := bytes.Repeat([]byte("f"), replica.DefaultBlockSize)
	device.SetRemoteFlushHandler(func(generation *replica.Generation) <-chan error {
		if got := generation.Blocks[0]; !bytes.Equal(got, payload) {
			return completedFlush(errors.New("FUA generation does not contain its write"))
		}
		return completedFlush(nil)
	})
	client, server := net.Pipe()
	setNBDTestDeadline(t, client)
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, false) }()

	writeRequest(t, client, nbdCmdWrite|nbdCmdFlagFUA, 1, 0, uint32(len(payload)), payload)
	readReply(t, client, 1, nil)
	writeRequest(t, client, nbdCmdDisconnect, 2, 0, 0, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNBDAllowsLargeTrimRequests(t *testing.T) {
	size := int64(maxNBDRequest + replica.DefaultBlockSize)
	device, err := replica.OpenDevice(filepath.Join(t.TempDir(), "image"), size, size)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- serveNBD(server, device, false) }()

	writeRequest(t, client, nbdCmdTrim, 1, 0, uint32(size), nil)
	readReply(t, client, 1, nil)
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

type nbdTestReply struct {
	handle uint64
	data   []byte
}

func readNBDReply(t *testing.T, conn net.Conn, payloadLength int) nbdTestReply {
	t.Helper()
	reply := make([]byte, 16+payloadLength)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint32(reply[:4]); got != nbdReplyMagic {
		t.Fatalf("reply magic = %#x", got)
	}
	if got := binary.BigEndian.Uint32(reply[4:8]); got != 0 {
		t.Fatalf("reply errno = %d", got)
	}
	return nbdTestReply{handle: binary.BigEndian.Uint64(reply[8:16]), data: reply[16:]}
}

func readReply(t *testing.T, conn net.Conn, handle uint64, payload []byte) {
	t.Helper()
	reply := readNBDReply(t, conn, len(payload))
	if reply.handle != handle {
		got := reply.handle
		t.Fatalf("reply handle = %d, want %d", got, handle)
	}
	if !bytes.Equal(reply.data, payload) {
		t.Fatal("reply data differs")
	}
}

func setNBDTestDeadline(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.SetDeadline(time.Time{}) })
}

func completedFlush(err error) <-chan error {
	result := make(chan error, 1)
	result <- err
	return result
}
