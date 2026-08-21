package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (db *DB) Pack(ctx context.Context, dir string) error {
	return db.Do(ctx, func(tx *Tx) error {
		if err := tx.clear(); err != nil {
			return err
		}
		if _, err := tx.ImportTree(dir); err != nil {
			return fmt.Errorf("pack %s: %w", dir, err)
		}
		return tx.SetMeta("packed_at", time.Now().UTC().Format(time.RFC3339Nano))
	})
}

func (tx *Tx) clear() error {
	for _, q := range []string{
		`DELETE FROM chunks`, `DELETE FROM xattrs`, `DELETE FROM dentries`,
		`DELETE FROM inodes WHERE ino != ` + fmt.Sprint(RootIno),
	} {
		if _, err := tx.exec(q); err != nil {
			return err
		}
	}
	return nil
}

func (tx *Tx) ImportTree(dir string) (int64, error) {
	var count int64
	inoByPath := map[string]uint64{".": RootIno}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || rel == "." {
			return err
		}
		parent := inoByPath[filepath.Dir(rel)]
		info, err := d.Info()
		if err != nil {
			return err
		}
		ino, err := tx.importEntry(parent, path, filepath.Base(rel), info)
		if err != nil {
			return err
		}
		if ino != 0 {
			inoByPath[rel] = ino
			count++
		}
		return nil
	})
	return count, err
}

func (tx *Tx) EnsureDir(p string) (uint64, error) {
	p = strings.Trim(p, "/")
	if p == "" || p == "." {
		return RootIno, nil
	}
	ino := uint64(RootIno)
	for _, part := range strings.Split(p, "/") {
		attr, err := tx.Lookup(ino, part)
		if errors.Is(err, ErrNotFound) {
			attr, err = tx.Create(ino, part, NewInode{Mode: dirMode | 0o755})
		}
		if err != nil {
			return 0, err
		}
		if !attr.IsDir() {
			return 0, ErrNotDir
		}
		ino = attr.Ino
	}
	return ino, nil
}

func (tx *Tx) WriteFrom(ino uint64, r io.Reader) error {
	buf := make([]byte, tx.db.chunkSize)
	var off int64
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if _, werr := tx.WriteAt(ino, buf[:n], off); werr != nil {
				return werr
			}
			off += int64(n)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (tx *Tx) importEntry(parent uint64, path, name string, info fs.FileInfo) (uint64, error) {
	spec := NewInode{Mode: ModeFromFS(info.Mode()), Time: info.ModTime()}
	if stat := ownerOf(info); stat != nil {
		spec.Uid, spec.Gid = stat.uid, stat.gid
	}
	switch {
	case info.Mode().IsDir():
		attr, err := tx.Create(parent, name, spec)
		if err != nil {
			return 0, err
		}
		return attr.Ino, nil
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return 0, err
		}
		spec.Target = target
		attr, err := tx.Create(parent, name, spec)
		return attr.Ino, err
	case info.Mode().IsRegular():
		attr, err := tx.Create(parent, name, spec)
		if err != nil {
			return 0, err
		}
		if err := tx.importFileData(attr.Ino, path); err != nil {
			return 0, err
		}
		mtime := info.ModTime()
		_, err = tx.SetAttr(attr.Ino, AttrChange{Mtime: &mtime})
		return attr.Ino, err
	default:
		return 0, nil
	}
}

func (tx *Tx) importFileData(ino uint64, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return tx.WriteFrom(ino, f)
}

func (db *DB) Unpack(ctx context.Context, dir string) error {
	return db.View(ctx, func(tx *Tx) error {
		return tx.exportTree(RootIno, dir)
	})
}

func (tx *Tx) exportTree(ino uint64, dir string) error {
	entries, err := tx.Readdir(ino)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		attr, err := tx.Stat(entry.Ino)
		if err != nil {
			return err
		}
		target := filepath.Join(dir, entry.Name)
		if err := tx.exportEntry(attr, target); err != nil {
			return fmt.Errorf("unpack %s: %w", entry.Name, err)
		}
	}
	return nil
}

func (tx *Tx) exportEntry(attr Attr, target string) error {
	perm := ModeToFS(attr.Mode).Perm()
	switch {
	case attr.IsDir():
		if err := os.MkdirAll(target, 0o700); err != nil {
			return err
		}
		if err := tx.exportTree(attr.Ino, target); err != nil {
			return err
		}
		return os.Chmod(target, perm)
	case attr.IsSymlink():
		os.Remove(target)
		return os.Symlink(attr.Target, target)
	default:
		f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
		if err != nil {
			return err
		}
		buf := make([]byte, tx.db.chunkSize)
		for off := int64(0); off < attr.Size; off += tx.db.chunkSize {
			n, err := tx.ReadAt(attr.Ino, buf, off)
			if err != nil {
				f.Close()
				return err
			}
			if _, err := f.Write(buf[:n]); err != nil {
				f.Close()
				return err
			}
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := os.Chmod(target, perm); err != nil {
			return err
		}
		return os.Chtimes(target, attr.Mtime, attr.Mtime)
	}
}

type Entry struct {
	Path  string
	Mode  fs.FileMode
	MTime time.Time
	Size  int64
}

func (db *DB) ListFiles(ctx context.Context) ([]Entry, error) {
	var entries []Entry
	err := db.View(ctx, func(tx *Tx) error {
		return tx.walk(RootIno, "", func(path string, attr Attr) error {
			entries = append(entries, Entry{Path: path, Mode: ModeToFS(attr.Mode), MTime: attr.Mtime, Size: attr.Size})
			return nil
		})
	})
	return entries, err
}

func (tx *Tx) walk(ino uint64, prefix string, fn func(path string, attr Attr) error) error {
	entries, err := tx.Readdir(ino)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		attr, err := tx.Stat(entry.Ino)
		if err != nil {
			return err
		}
		path := entry.Name
		if prefix != "" {
			path = prefix + "/" + entry.Name
		}
		if err := fn(path, attr); err != nil {
			return err
		}
		if attr.IsDir() {
			if err := tx.walk(entry.Ino, path, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tx *Tx) migrateFromV0() error {
	if err := tx.bootstrap(); err != nil {
		return err
	}
	rows, err := tx.query(`SELECT path, mode, mtime, data FROM files ORDER BY path`)
	if err != nil {
		return err
	}
	type legacy struct {
		path  string
		mode  fs.FileMode
		mtime time.Time
		data  []byte
	}
	var files []legacy
	for rows.Next() {
		var l legacy
		var mode uint32
		var mtime int64
		if err := rows.Scan(&l.path, &mode, &mtime, &l.data); err != nil {
			rows.Close()
			return err
		}
		l.mode, l.mtime = fs.FileMode(mode), time.Unix(0, mtime)
		files = append(files, l)
	}
	rows.Close()

	inoByPath := map[string]uint64{".": RootIno}
	for _, l := range files {
		if strings.Contains(l.path, "..") {
			return fmt.Errorf("refusing unsafe legacy path %q", l.path)
		}
		parent, ok := inoByPath[filepath.Dir(l.path)]
		if !ok {
			return fmt.Errorf("legacy entry %q has no parent", l.path)
		}
		spec := NewInode{Mode: ModeFromFS(l.mode), Time: l.mtime}
		if l.mode&fs.ModeSymlink != 0 {
			spec.Target = string(l.data)
		}
		attr, err := tx.Create(parent, filepath.Base(l.path), spec)
		if err != nil {
			return fmt.Errorf("migrate %q: %w", l.path, err)
		}
		inoByPath[l.path] = attr.Ino
		if l.mode.IsRegular() {
			if _, err := tx.WriteAt(attr.Ino, l.data, 0); err != nil {
				return err
			}
			if _, err := tx.SetAttr(attr.Ino, AttrChange{Mtime: &l.mtime}); err != nil {
				return err
			}
		}
	}
	if _, err := tx.exec(`DROP TABLE files`); err != nil {
		return err
	}
	return tx.SetMeta("schema_version", SchemaVersion)
}
