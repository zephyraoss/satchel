package seed

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type Fetcher func(ctx context.Context, bucket, key string) (io.ReadCloser, error)

type Seeder struct {
	Fetch Fetcher
}

func (s *Seeder) Apply(ctx context.Context, destination, source string) (int64, error) {
	if strings.HasPrefix(source, "s3://") {
		return s.applyS3(ctx, destination, source)
	}
	resolved, err := filepath.EvalSymlinks(source)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return 0, err
	}
	if info.IsDir() {
		return importDir(ctx, destination, resolved)
	}
	file, err := os.Open(resolved)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return importArchive(ctx, destination, file, strings.HasSuffix(source, ".gz") || strings.HasSuffix(source, ".tgz"))
}

func (s *Seeder) applyS3(ctx context.Context, destination, source string) (int64, error) {
	if s.Fetch == nil {
		return 0, errors.New("no S3 fetcher configured")
	}
	location, err := url.Parse(source)
	if err != nil {
		return 0, err
	}
	key := strings.TrimPrefix(location.Path, "/")
	body, err := s.Fetch(ctx, location.Host, key)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	return importArchive(ctx, destination, body, strings.HasSuffix(key, ".gz") || strings.HasSuffix(key, ".tgz"))
}

func importDir(ctx context.Context, destination, source string) (int64, error) {
	var count int64
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil || rel == "." {
			return err
		}
		target, err := safePath(destination, filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		switch {
		case entry.IsDir():
			err = os.MkdirAll(target, info.Mode().Perm())
		case entry.Type()&os.ModeSymlink != 0:
			var link string
			link, err = os.Readlink(path)
			if err == nil {
				err = os.Symlink(link, target)
			}
		case info.Mode().IsRegular():
			err = copyFile(path, target, info.Mode().Perm())
		default:
			return nil
		}
		if err == nil {
			count++
		}
		return err
	})
	return count, err
}

func importArchive(ctx context.Context, destination string, reader io.Reader, compressed bool) (int64, error) {
	if compressed {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return 0, err
		}
		defer gz.Close()
		reader = gz
	}
	archive := tar.NewReader(reader)
	var count int64
	var directories []*tar.Header
	for {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return count, err
		}
		target, err := safePath(destination, header.Name)
		if err != nil {
			return count, fmt.Errorf("tar entry %q: %w", header.Name, err)
		}
		if target == destination {
			continue
		}
		if err := ensureSafeParents(destination, filepath.Dir(target)); err != nil {
			return count, fmt.Errorf("tar entry %q: %w", header.Name, err)
		}
		switch header.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, os.FileMode(header.Mode).Perm())
		case tar.TypeSymlink:
			err = os.Symlink(header.Linkname, target)
		case tar.TypeReg, tar.TypeRegA:
			err = writeFile(archive, target, os.FileMode(header.Mode).Perm())
		default:
			continue
		}
		if err != nil {
			return count, fmt.Errorf("tar entry %q: %w", header.Name, err)
		}
		switch header.Typeflag {
		case tar.TypeDir:
			directories = append(directories, header)
		case tar.TypeSymlink:
		default:
			applyMetadata(target, header)
		}
		count++
	}
	for index := len(directories) - 1; index >= 0; index-- {
		header := directories[index]
		target, _ := safePath(destination, header.Name)
		applyMetadata(target, header)
	}
	return count, nil
}

func applyMetadata(target string, header *tar.Header) {
	_ = os.Chown(target, header.Uid, header.Gid)
	_ = os.Chtimes(target, header.AccessTime, header.ModTime)
}

func safePath(root, name string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(name, "/")))
	if clean == "." || clean == "" {
		return root, nil
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes archive root")
	}
	return filepath.Join(root, clean), nil
}

func ensureSafeParents(root, parent string) error {
	rel, err := filepath.Rel(root, parent)
	if err != nil {
		return err
	}
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "." || part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0o755); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("archive parent is not a directory")
		}
	}
	return nil
}

func copyFile(source, destination string, mode os.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	return writeFile(in, destination, mode)
}

func writeFile(reader io.Reader, destination string, mode os.FileMode) error {
	if info, err := os.Lstat(destination); err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("refusing to write through %s: not a regular file", destination)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|unix.O_NOFOLLOW, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, reader)
	closeErr := out.Close()
	return errors.Join(copyErr, closeErr)
}
