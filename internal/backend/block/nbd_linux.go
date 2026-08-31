//go:build linux

package block

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/zephyraoss/satchel/internal/replica"
)

const (
	nbdSetSock       = 0xab00
	nbdSetBlockSize  = 0xab01
	nbdDoIt          = 0xab03
	nbdClearSock     = 0xab04
	nbdClearQueue    = 0xab05
	nbdSetSizeBlocks = 0xab07
	nbdDisconnect    = 0xab08
	nbdSetFlags      = 0xab0a

	nbdRequestMagic = 0x25609513
	nbdReplyMagic   = 0x67446698

	nbdCmdRead        = 0
	nbdCmdWrite       = 1
	nbdCmdDisconnect  = 2
	nbdCmdFlush       = 3
	nbdCmdTrim        = 4
	nbdCmdWriteZeroes = 6
	nbdCmdMask        = 0xffff
	nbdCmdFlagFUA     = 1 << 16

	nbdFlagHasFlags       = 1 << 0
	nbdFlagReadOnly       = 1 << 1
	nbdFlagSendFlush      = 1 << 2
	nbdFlagSendFUA        = 1 << 3
	nbdFlagSendTrim       = 1 << 5
	nbdFlagSendWriteZeros = 1 << 6

	maxNBDRequest = 4 << 20
)

type Attachment struct {
	path       string
	device     *os.File
	server     net.Conn
	done       chan error
	kernelDone chan error
	stopOnce   sync.Once
	closeErr   error
}

func Attach(ctx context.Context, device *replica.Device, readOnly bool) (*Attachment, error) {
	for i := 0; i < 256; i++ {
		path := fmt.Sprintf("/dev/nbd%d", i)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if _, err := os.Stat(filepath.Join("/sys/block", filepath.Base(path), "pid")); err == nil {
			continue
		}
		attachment, err := attachAt(ctx, path, device, readOnly)
		if errors.Is(err, unix.EBUSY) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return attachment, nil
	}
	return nil, errors.New("no free /dev/nbd device; load the nbd module with nbds_max large enough")
}

func attachAt(ctx context.Context, path string, backing *replica.Device, readOnly bool) (*Attachment, error) {
	dev, err := os.OpenFile(path, os.O_RDWR|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	closeDev := true
	defer func() {
		if closeDev {
			dev.Close()
		}
	}()
	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	closeSockets := true
	defer func() {
		if closeSockets {
			unix.Close(sockets[0])
			unix.Close(sockets[1])
		}
	}()
	if err := ioctlInt(dev.Fd(), nbdSetSock, sockets[0]); err != nil {
		return nil, err
	}
	configured := true
	defer func() {
		if configured {
			_ = ioctlInt(dev.Fd(), nbdClearSock, 0)
		}
	}()
	if err := ioctlInt(dev.Fd(), nbdSetBlockSize, replica.DefaultBlockSize); err != nil {
		return nil, err
	}
	if err := ioctlInt(dev.Fd(), nbdSetSizeBlocks, int(backing.Size()/replica.DefaultBlockSize)); err != nil {
		return nil, err
	}
	flags := nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendFUA | nbdFlagSendTrim | nbdFlagSendWriteZeros
	if readOnly {
		flags |= nbdFlagReadOnly
	}
	if err := ioctlInt(dev.Fd(), nbdSetFlags, flags); err != nil {
		return nil, err
	}

	unix.Close(sockets[0])
	serverFile := os.NewFile(uintptr(sockets[1]), "satchel-nbd-server")
	server, err := net.FileConn(serverFile)
	serverFile.Close()
	if err != nil {
		return nil, err
	}
	closeSockets = false
	attachment := &Attachment{path: path, device: dev, server: server, done: make(chan error, 1), kernelDone: make(chan error, 1)}
	go func() {
		serveErr := serveNBD(server, backing, readOnly)
		_ = server.Close()
		attachment.done <- serveErr
	}()
	go func() {
		err := ioctlInt(dev.Fd(), nbdDoIt, 0)
		if err != nil && !errors.Is(err, unix.EPIPE) && !errors.Is(err, unix.EINVAL) {
			_ = server.Close()
		}
		attachment.kernelDone <- err
	}()

	name := filepath.Base(path)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, pidErr := os.ReadFile(filepath.Join("/sys/block", name, "pid"))
		if data, err := os.ReadFile(filepath.Join("/sys/block", name, "size")); pidErr == nil && err == nil {
			sectors, parseErr := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if parseErr == nil && sectors == backing.Size()/512 {
				configured = false
				closeDev = false
				return attachment, nil
			}
		}
		select {
		case err := <-attachment.done:
			_ = ioctlInt(dev.Fd(), nbdDisconnect, 0)
			return nil, fmt.Errorf("start NBD server: %w", err)
		case <-ctx.Done():
			_ = ioctlInt(dev.Fd(), nbdDisconnect, 0)
			_ = server.Close()
			return nil, ctx.Err()
		case <-deadline.C:
			_ = ioctlInt(dev.Fd(), nbdDisconnect, 0)
			_ = server.Close()
			return nil, fmt.Errorf("timed out attaching %s", path)
		case <-ticker.C:
		}
	}
}

func ioctlInt(fd uintptr, request uintptr, value int) error {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, fd, request, uintptr(value))
	if errno != 0 {
		return errno
	}
	return nil
}

func (a *Attachment) Path() string { return a.path }

func (a *Attachment) Close() error {
	a.stopOnce.Do(func() {
		if err := ioctlInt(a.device.Fd(), nbdDisconnect, 0); err != nil && !errors.Is(err, unix.EINVAL) {
			a.closeErr = err
		}
		_ = a.server.Close()
		select {
		case err := <-a.done:
			if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) && a.closeErr == nil {
				a.closeErr = err
			}
		case <-time.After(5 * time.Second):
			if a.closeErr == nil {
				a.closeErr = errors.New("NBD server did not stop")
			}
		}
		select {
		case err := <-a.kernelDone:
			if err != nil && !errors.Is(err, unix.EPIPE) && !errors.Is(err, unix.EINVAL) && a.closeErr == nil {
				a.closeErr = err
			}
		case <-time.After(5 * time.Second):
			if a.closeErr == nil {
				a.closeErr = errors.New("NBD kernel client did not stop")
			}
		}
		_ = ioctlInt(a.device.Fd(), nbdClearQueue, 0)
		_ = ioctlInt(a.device.Fd(), nbdClearSock, 0)
		if err := a.device.Close(); err != nil && a.closeErr == nil {
			a.closeErr = err
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			_, pidErr := os.Stat(filepath.Join("/sys/block", filepath.Base(a.path), "pid"))
			sizeData, sizeErr := os.ReadFile(filepath.Join("/sys/block", filepath.Base(a.path), "size"))
			if errors.Is(pidErr, os.ErrNotExist) && sizeErr == nil && strings.TrimSpace(string(sizeData)) == "0" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	return a.closeErr
}

func serveNBD(conn io.ReadWriter, device *replica.Device, readOnly bool) error {
	header := make([]byte, 28)
	for {
		if _, err := io.ReadFull(conn, header); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(header[0:4]) != nbdRequestMagic {
			return errors.New("invalid NBD request magic")
		}
		typeAndFlags := binary.BigEndian.Uint32(header[4:8])
		command := typeAndFlags & nbdCmdMask
		handle := append([]byte(nil), header[8:16]...)
		offset := binary.BigEndian.Uint64(header[16:24])
		length := binary.BigEndian.Uint32(header[24:28])
		if command == nbdCmdDisconnect {
			return nil
		}
		if length > maxNBDRequest || offset > uint64(device.Size()) || uint64(length) > uint64(device.Size())-offset {
			if command == nbdCmdWrite {
				if _, err := io.CopyN(io.Discard, conn, int64(length)); err != nil {
					return err
				}
			}
			if err := writeReply(conn, handle, unix.EINVAL, nil); err != nil {
				return err
			}
			continue
		}

		var data []byte
		var requestErr error
		switch command {
		case nbdCmdRead:
			data = make([]byte, length)
			_, requestErr = device.ReadAt(data, int64(offset))
		case nbdCmdWrite:
			data = make([]byte, length)
			if _, requestErr = io.ReadFull(conn, data); requestErr == nil {
				if readOnly {
					requestErr = unix.EROFS
				} else {
					_, requestErr = device.WriteAt(data, int64(offset))
				}
			}
			data = nil
			if requestErr == nil && typeAndFlags&nbdCmdFlagFUA != 0 {
				requestErr = device.Flush()
			}
		case nbdCmdFlush:
			requestErr = device.Flush()
		case nbdCmdTrim, nbdCmdWriteZeroes:
			if readOnly {
				requestErr = unix.EROFS
			} else {
				requestErr = device.Trim(int64(offset), int64(length))
			}
		default:
			requestErr = unix.EINVAL
		}
		if err := writeReply(conn, handle, errno(requestErr), data); err != nil {
			return err
		}
	}
}

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var value syscall.Errno
	if errors.As(err, &value) {
		return value
	}
	return unix.EIO
}

func writeReply(w io.Writer, handle []byte, errno syscall.Errno, data []byte) error {
	reply := make([]byte, 16)
	binary.BigEndian.PutUint32(reply[0:4], nbdReplyMagic)
	binary.BigEndian.PutUint32(reply[4:8], uint32(errno))
	copy(reply[8:16], handle)
	if err := writeFull(w, reply); err != nil {
		return err
	}
	if errno == 0 && len(data) > 0 {
		return writeFull(w, data)
	}
	return nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
