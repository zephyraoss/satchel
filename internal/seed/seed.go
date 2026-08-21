package seed

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/zephyraoss/satchel/internal/store"
)

type Fetcher func(ctx context.Context, bucket, key string) (io.ReadCloser, error)

type Seeder struct {
	Fetch Fetcher
}

func (s *Seeder) Apply(ctx context.Context, db *store.DB, source string) (int64, error) {
	if strings.HasPrefix(source, "s3://") {
		return s.applyS3(ctx, db, source)
	}
	info, err := os.Stat(source)
	if err != nil {
		return 0, err
	}
	if info.IsDir() {
		return importDir(ctx, db, source)
	}
	f, err := os.Open(source)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return importArchive(ctx, db, f, strings.HasSuffix(source, ".gz") || strings.HasSuffix(source, ".tgz"))
}

func (s *Seeder) applyS3(ctx context.Context, db *store.DB, source string) (int64, error) {
	if s.Fetch == nil {
		return 0, errors.New("no S3 fetcher configured")
	}
	u, err := url.Parse(source)
	if err != nil {
		return 0, err
	}
	key := strings.TrimPrefix(u.Path, "/")
	body, err := s.Fetch(ctx, u.Host, key)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	return importArchive(ctx, db, body, strings.HasSuffix(key, ".gz") || strings.HasSuffix(key, ".tgz"))
}

func importDir(ctx context.Context, db *store.DB, dir string) (int64, error) {
	var count int64
	err := db.Do(ctx, func(tx *store.Tx) error {
		n, err := tx.ImportTree(dir)
		count = n
		return err
	})
	return count, err
}

func importArchive(ctx context.Context, db *store.DB, r io.Reader, gzipped bool) (int64, error) {
	if gzipped {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return 0, err
		}
		defer gz.Close()
		r = gz
	}
	tr := tar.NewReader(r)
	var count int64
	err := db.Do(ctx, func(tx *store.Tx) error {
		for {
			hdr, err := tr.Next()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := importTarEntry(tx, tr, hdr); err != nil {
				return fmt.Errorf("tar entry %q: %w", hdr.Name, err)
			}
			count++
		}
	})
	return count, err
}

func importTarEntry(tx *store.Tx, tr *tar.Reader, hdr *tar.Header) error {
	name := path.Clean(strings.TrimPrefix(hdr.Name, "/"))
	if name == "." || name == "" {
		return nil
	}
	if strings.HasPrefix(name, "../") || name == ".." {
		return errors.New("path escapes archive root")
	}
	parent, err := tx.EnsureDir(path.Dir(name))
	if err != nil {
		return err
	}
	base := path.Base(name)
	spec := store.NewInode{Uid: uint32(hdr.Uid), Gid: uint32(hdr.Gid), Time: hdr.ModTime}
	switch hdr.Typeflag {
	case tar.TypeDir:
		spec.Mode = store.ModeFromFS(os.ModeDir | os.FileMode(hdr.Mode).Perm())
		_, err := tx.Create(parent, base, spec)
		if errors.Is(err, store.ErrExists) {
			return nil
		}
		return err
	case tar.TypeSymlink:
		spec.Mode = store.ModeFromFS(os.ModeSymlink | 0o777)
		spec.Target = hdr.Linkname
		_, err := tx.Create(parent, base, spec)
		return err
	case tar.TypeReg, tar.TypeRegA:
		spec.Mode = store.ModeFromFS(os.FileMode(hdr.Mode).Perm())
		attr, err := tx.Create(parent, base, spec)
		if err != nil {
			return err
		}
		if err := tx.WriteFrom(attr.Ino, tr); err != nil {
			return err
		}
		mtime := hdr.ModTime
		_, err = tx.SetAttr(attr.Ino, store.AttrChange{Mtime: &mtime})
		return err
	default:
		return nil
	}
}
