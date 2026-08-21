package store

import (
	"io/fs"
	"syscall"
)

type owner struct{ uid, gid uint32 }

func ownerOf(info fs.FileInfo) *owner {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	return &owner{uid: stat.Uid, gid: stat.Gid}
}
