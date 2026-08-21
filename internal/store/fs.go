package store

import (
	"database/sql"
	"errors"
	"io/fs"
	"syscall"
	"time"
)

const (
	typeMask    = uint32(syscall.S_IFMT)
	dirMode     = uint32(syscall.S_IFDIR)
	regMode     = uint32(syscall.S_IFREG)
	symlinkMode = uint32(syscall.S_IFLNK)
)

var (
	ErrNotFound  = syscall.ENOENT
	ErrExists    = syscall.EEXIST
	ErrNotEmpty  = syscall.ENOTEMPTY
	ErrNotDir    = syscall.ENOTDIR
	ErrIsDir     = syscall.EISDIR
	ErrNoXattr   = syscall.ENODATA
	ErrInvalid   = syscall.EINVAL
	ErrNameTooLg = syscall.ENAMETOOLONG
)

type Attr struct {
	Ino    uint64
	Mode   uint32
	Uid    uint32
	Gid    uint32
	Size   int64
	Atime  time.Time
	Mtime  time.Time
	Ctime  time.Time
	Nlink  uint32
	Target string
}

func (a Attr) IsDir() bool     { return a.Mode&typeMask == dirMode }
func (a Attr) IsSymlink() bool { return a.Mode&typeMask == symlinkMode }
func (a Attr) IsRegular() bool { return a.Mode&typeMask == regMode }

type DirEntry struct {
	Name string
	Ino  uint64
	Mode uint32
}

type NewInode struct {
	Mode   uint32
	Uid    uint32
	Gid    uint32
	Target string
	Time   time.Time
}

const attrColumns = `ino, mode, uid, gid, size, atime, mtime, ctime, nlink, COALESCE(target, '')`

const (
	qStat          = `SELECT ` + attrColumns + ` FROM inodes WHERE ino = ?`
	qLookup        = `SELECT ` + attrColumns + ` FROM inodes WHERE ino = (SELECT ino FROM dentries WHERE parent = ? AND name = ?)`
	qReaddir       = `SELECT d.name, d.ino, i.mode FROM dentries d JOIN inodes i ON i.ino = d.ino WHERE d.parent = ? ORDER BY d.name`
	qInsertInode   = `INSERT INTO inodes(mode, uid, gid, size, atime, mtime, ctime, nlink, target) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	qInsertDentry  = `INSERT INTO dentries(parent, name, ino) VALUES (?, ?, ?)`
	qTouchDir      = `UPDATE inodes SET mtime = ?, ctime = ? WHERE ino = ?`
	qDeleteDentry  = `DELETE FROM dentries WHERE parent = ? AND name = ?`
	qDecNlink      = `UPDATE inodes SET nlink = nlink - 1, ctime = ? WHERE ino = ?`
	qDeleteChunks  = `DELETE FROM chunks WHERE ino = ?`
	qDeleteXattrs  = `DELETE FROM xattrs WHERE ino = ?`
	qDeleteInode   = `DELETE FROM inodes WHERE ino = ?`
	qCountChildren = `SELECT COUNT(*) FROM dentries WHERE parent = ?`
	qSelectChunks  = `SELECT idx, data FROM chunks WHERE ino = ? AND idx BETWEEN ? AND ? ORDER BY idx`
	qSelectChunk   = `SELECT data FROM chunks WHERE ino = ? AND idx = ?`
	qUpsertChunk   = `INSERT INTO chunks(ino, idx, data) VALUES (?, ?, ?) ON CONFLICT(ino, idx) DO UPDATE SET data = excluded.data`
	qGrowInode     = `UPDATE inodes SET size = MAX(size, ?), mtime = ?, ctime = ? WHERE ino = ?`
	qSetCtime      = `UPDATE inodes SET ctime = ? WHERE ino = ?`
	qSetSize       = `UPDATE inodes SET size = ? WHERE ino = ?`
	qGetXattr      = `SELECT value FROM xattrs WHERE ino = ? AND name = ?`
)

var hotQueries = []string{qStat, qLookup, qReaddir, qInsertInode, qInsertDentry, qTouchDir, qDeleteDentry, qDecNlink, qDeleteChunks, qDeleteXattrs, qDeleteInode, qCountChildren, qSelectChunks, qSelectChunk, qUpsertChunk, qGrowInode, qSetCtime, qSetSize, qGetXattr}

func scanAttr(row interface{ Scan(...any) error }) (Attr, error) {
	var a Attr
	var atime, mtime, ctime int64
	if err := row.Scan(&a.Ino, &a.Mode, &a.Uid, &a.Gid, &a.Size, &atime, &mtime, &ctime, &a.Nlink, &a.Target); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Attr{}, ErrNotFound
		}
		return Attr{}, err
	}
	a.Atime, a.Mtime, a.Ctime = time.Unix(0, atime), time.Unix(0, mtime), time.Unix(0, ctime)
	return a, nil
}

func (tx *Tx) Stat(ino uint64) (Attr, error) {
	return scanAttr(tx.queryRow(qStat, ino))
}

func (tx *Tx) Lookup(parent uint64, name string) (Attr, error) {
	return scanAttr(tx.queryRow(qLookup, parent, name))
}

func (tx *Tx) Readdir(parent uint64) ([]DirEntry, error) {
	rows, err := tx.query(qReaddir, parent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []DirEntry
	for rows.Next() {
		var e DirEntry
		if err := rows.Scan(&e.Name, &e.Ino, &e.Mode); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." {
		return ErrInvalid
	}
	if len(name) > 255 {
		return ErrNameTooLg
	}
	for i := 0; i < len(name); i++ {
		if name[i] == '/' || name[i] == 0 {
			return ErrInvalid
		}
	}
	return nil
}

func (tx *Tx) Create(parent uint64, name string, spec NewInode) (Attr, error) {
	if err := validName(name); err != nil {
		return Attr{}, err
	}
	dir, err := tx.Stat(parent)
	if err != nil {
		return Attr{}, err
	}
	if !dir.IsDir() {
		return Attr{}, ErrNotDir
	}
	if _, err := tx.Lookup(parent, name); err == nil {
		return Attr{}, ErrExists
	} else if !errors.Is(err, ErrNotFound) {
		return Attr{}, err
	}
	if spec.Time.IsZero() {
		spec.Time = time.Now()
	}
	now := spec.Time.UnixNano()
	nlink := 1
	size := int64(0)
	if spec.Mode&typeMask == dirMode {
		nlink = 2
	}
	if spec.Mode&typeMask == symlinkMode {
		size = int64(len(spec.Target))
	}
	var target any
	if spec.Target != "" {
		target = spec.Target
	}
	res, err := tx.exec(qInsertInode,
		spec.Mode, spec.Uid, spec.Gid, size, now, now, now, nlink, target)
	if err != nil {
		return Attr{}, err
	}
	ino, err := res.LastInsertId()
	if err != nil {
		return Attr{}, err
	}
	if _, err := tx.exec(qInsertDentry, parent, name, ino); err != nil {
		return Attr{}, err
	}
	if err := tx.touchDir(parent, now); err != nil {
		return Attr{}, err
	}
	return tx.Stat(uint64(ino))
}

func (tx *Tx) touchDir(ino uint64, now int64) error {
	_, err := tx.exec(qTouchDir, now, now, ino)
	return err
}

func (tx *Tx) Link(ino, newParent uint64, newName string) (Attr, error) {
	if err := validName(newName); err != nil {
		return Attr{}, err
	}
	target, err := tx.Stat(ino)
	if err != nil {
		return Attr{}, err
	}
	if target.IsDir() {
		return Attr{}, syscall.EPERM
	}
	if _, err := tx.Lookup(newParent, newName); err == nil {
		return Attr{}, ErrExists
	} else if !errors.Is(err, ErrNotFound) {
		return Attr{}, err
	}
	now := time.Now().UnixNano()
	if _, err := tx.exec(qInsertDentry, newParent, newName, ino); err != nil {
		return Attr{}, err
	}
	if _, err := tx.exec(`UPDATE inodes SET nlink = nlink + 1, ctime = ? WHERE ino = ?`, now, ino); err != nil {
		return Attr{}, err
	}
	if err := tx.touchDir(newParent, now); err != nil {
		return Attr{}, err
	}
	return tx.Stat(ino)
}

func (tx *Tx) Unlink(parent uint64, name string, keepOrphan bool) (Attr, error) {
	child, err := tx.Lookup(parent, name)
	if err != nil {
		return Attr{}, err
	}
	if child.IsDir() {
		return Attr{}, ErrIsDir
	}
	now := time.Now().UnixNano()
	if _, err := tx.exec(qDeleteDentry, parent, name); err != nil {
		return Attr{}, err
	}
	if _, err := tx.exec(qDecNlink, now, child.Ino); err != nil {
		return Attr{}, err
	}
	if err := tx.touchDir(parent, now); err != nil {
		return Attr{}, err
	}
	child.Nlink--
	if child.Nlink == 0 && !keepOrphan {
		return child, tx.DeleteInode(child.Ino)
	}
	return child, nil
}

func (tx *Tx) DeleteInode(ino uint64) error {
	for _, q := range []string{qDeleteChunks, qDeleteXattrs, qDeleteInode} {
		if _, err := tx.exec(q, ino); err != nil {
			return err
		}
	}
	return nil
}

func (tx *Tx) CollectOrphans() (int64, error) {
	rows, err := tx.query(`SELECT ino FROM inodes WHERE ino != ? AND ino NOT IN (SELECT ino FROM dentries)`, RootIno)
	if err != nil {
		return 0, err
	}
	var orphans []uint64
	for rows.Next() {
		var ino uint64
		if err := rows.Scan(&ino); err != nil {
			rows.Close()
			return 0, err
		}
		orphans = append(orphans, ino)
	}
	rows.Close()
	for _, ino := range orphans {
		if err := tx.DeleteInode(ino); err != nil {
			return 0, err
		}
	}
	return int64(len(orphans)), nil
}

func (tx *Tx) Rmdir(parent uint64, name string) error {
	child, err := tx.Lookup(parent, name)
	if err != nil {
		return err
	}
	if !child.IsDir() {
		return ErrNotDir
	}
	empty, err := tx.isEmptyDir(child.Ino)
	if err != nil {
		return err
	}
	if !empty {
		return ErrNotEmpty
	}
	if _, err := tx.exec(qDeleteDentry, parent, name); err != nil {
		return err
	}
	if err := tx.DeleteInode(child.Ino); err != nil {
		return err
	}
	return tx.touchDir(parent, time.Now().UnixNano())
}

func (tx *Tx) isEmptyDir(ino uint64) (bool, error) {
	var count int
	if err := tx.queryRow(qCountChildren, ino).Scan(&count); err != nil {
		return false, err
	}
	return count == 0, nil
}

func (tx *Tx) isAncestor(candidate, of uint64) (bool, error) {
	current := of
	for current != RootIno {
		if current == candidate {
			return true, nil
		}
		var parent uint64
		err := tx.queryRow(`SELECT parent FROM dentries WHERE ino = ? LIMIT 1`, current).Scan(&parent)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		current = parent
	}
	return candidate == RootIno, nil
}

type RenameOptions struct {
	NoReplace  bool
	KeepOrphan bool
}

func (tx *Tx) Rename(parent uint64, name string, newParent uint64, newName string, opts RenameOptions) error {
	if err := validName(newName); err != nil {
		return err
	}
	src, err := tx.Lookup(parent, name)
	if err != nil {
		return err
	}
	if parent == newParent && name == newName {
		return nil
	}
	if src.IsDir() {
		if loop, err := tx.isAncestor(src.Ino, newParent); err != nil {
			return err
		} else if loop {
			return ErrInvalid
		}
	}
	dst, err := tx.Lookup(newParent, newName)
	switch {
	case errors.Is(err, ErrNotFound):
	case err != nil:
		return err
	case opts.NoReplace:
		return ErrExists
	case dst.IsDir():
		if !src.IsDir() {
			return ErrIsDir
		}
		empty, err := tx.isEmptyDir(dst.Ino)
		if err != nil {
			return err
		}
		if !empty {
			return ErrNotEmpty
		}
		if _, err := tx.exec(qDeleteDentry, newParent, newName); err != nil {
			return err
		}
		if err := tx.DeleteInode(dst.Ino); err != nil {
			return err
		}
	default:
		if src.IsDir() {
			return ErrNotDir
		}
		if _, err := tx.Unlink(newParent, newName, opts.KeepOrphan); err != nil {
			return err
		}
	}
	now := time.Now().UnixNano()
	if _, err := tx.exec(`UPDATE dentries SET parent = ?, name = ? WHERE parent = ? AND name = ?`, newParent, newName, parent, name); err != nil {
		return err
	}
	if _, err := tx.exec(qSetCtime, now, src.Ino); err != nil {
		return err
	}
	if err := tx.touchDir(parent, now); err != nil {
		return err
	}
	return tx.touchDir(newParent, now)
}

type AttrChange struct {
	Mode  *uint32
	Uid   *uint32
	Gid   *uint32
	Size  *int64
	Atime *time.Time
	Mtime *time.Time
}

func (tx *Tx) SetAttr(ino uint64, change AttrChange) (Attr, error) {
	cur, err := tx.Stat(ino)
	if err != nil {
		return Attr{}, err
	}
	now := time.Now()
	if change.Size != nil {
		if cur.IsDir() {
			return Attr{}, ErrIsDir
		}
		if err := tx.truncate(ino, cur.Size, *change.Size); err != nil {
			return Attr{}, err
		}
		if change.Mtime == nil {
			change.Mtime = &now
		}
	}
	if change.Mode != nil {
		mode := cur.Mode&typeMask | *change.Mode&^typeMask
		if _, err := tx.exec(`UPDATE inodes SET mode = ? WHERE ino = ?`, mode, ino); err != nil {
			return Attr{}, err
		}
	}
	if change.Uid != nil {
		if _, err := tx.exec(`UPDATE inodes SET uid = ? WHERE ino = ?`, *change.Uid, ino); err != nil {
			return Attr{}, err
		}
	}
	if change.Gid != nil {
		if _, err := tx.exec(`UPDATE inodes SET gid = ? WHERE ino = ?`, *change.Gid, ino); err != nil {
			return Attr{}, err
		}
	}
	if change.Atime != nil {
		if _, err := tx.exec(`UPDATE inodes SET atime = ? WHERE ino = ?`, change.Atime.UnixNano(), ino); err != nil {
			return Attr{}, err
		}
	}
	if change.Mtime != nil {
		if _, err := tx.exec(`UPDATE inodes SET mtime = ? WHERE ino = ?`, change.Mtime.UnixNano(), ino); err != nil {
			return Attr{}, err
		}
	}
	if _, err := tx.exec(qSetCtime, now.UnixNano(), ino); err != nil {
		return Attr{}, err
	}
	return tx.Stat(ino)
}

func (tx *Tx) truncate(ino uint64, oldSize, newSize int64) error {
	if newSize < 0 {
		return ErrInvalid
	}
	if newSize < oldSize {
		if newSize == 0 {
			if _, err := tx.exec(qDeleteChunks, ino); err != nil {
				return err
			}
		} else {
			lastIdx := (newSize - 1) / tx.db.chunkSize
			keep := newSize - lastIdx*tx.db.chunkSize
			if _, err := tx.exec(`DELETE FROM chunks WHERE ino = ? AND idx > ?`, ino, lastIdx); err != nil {
				return err
			}
			if _, err := tx.exec(`UPDATE chunks SET data = substr(data, 1, ?) WHERE ino = ? AND idx = ? AND length(data) > ?`, keep, ino, lastIdx, keep); err != nil {
				return err
			}
		}
	}
	_, err := tx.exec(qSetSize, newSize, ino)
	return err
}

func (tx *Tx) ReadAt(ino uint64, dest []byte, off int64) (int, error) {
	attr, err := tx.Stat(ino)
	if err != nil {
		return 0, err
	}
	if attr.IsDir() {
		return 0, ErrIsDir
	}
	if off >= attr.Size {
		return 0, nil
	}
	if int64(len(dest)) > attr.Size-off {
		dest = dest[:attr.Size-off]
	}
	for i := range dest {
		dest[i] = 0
	}
	firstIdx, lastIdx := off/tx.db.chunkSize, (off+int64(len(dest))-1)/tx.db.chunkSize
	rows, err := tx.query(qSelectChunks, ino, firstIdx, lastIdx)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var idx int64
		var data []byte
		if err := rows.Scan(&idx, &data); err != nil {
			return 0, err
		}
		chunkStart := idx * tx.db.chunkSize
		srcStart := max(off-chunkStart, 0)
		dstStart := max(chunkStart-off, 0)
		if srcStart >= int64(len(data)) {
			continue
		}
		copy(dest[dstStart:], data[srcStart:])
	}
	return len(dest), rows.Err()
}

func (tx *Tx) WriteAt(ino uint64, data []byte, off int64) (int, error) {
	attr, err := tx.Stat(ino)
	if err != nil {
		return 0, err
	}
	if attr.IsDir() {
		return 0, ErrIsDir
	}
	if len(data) == 0 {
		return 0, nil
	}
	end := off + int64(len(data))
	firstIdx, lastIdx := off/tx.db.chunkSize, (end-1)/tx.db.chunkSize
	for idx := firstIdx; idx <= lastIdx; idx++ {
		chunkStart := idx * tx.db.chunkSize
		localStart := max(off-chunkStart, 0)
		localEnd := min(end-chunkStart, tx.db.chunkSize)
		srcStart := chunkStart + localStart - off
		piece := data[srcStart : srcStart+(localEnd-localStart)]
		if err := tx.patchChunk(ino, idx, localStart, piece); err != nil {
			return 0, err
		}
	}
	now := time.Now().UnixNano()
	if _, err := tx.exec(qGrowInode, end, now, now, ino); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (tx *Tx) patchChunk(ino uint64, idx, localStart int64, piece []byte) error {
	var existing []byte
	err := tx.queryRow(qSelectChunk, ino, idx).Scan(&existing)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if localStart == 0 && int64(len(piece)) >= int64(len(existing)) {
		_, err := tx.exec(qUpsertChunk, ino, idx, piece)
		return err
	}
	newLen := max(int64(len(existing)), localStart+int64(len(piece)))
	buf := make([]byte, newLen)
	copy(buf, existing)
	copy(buf[localStart:], piece)
	_, err = tx.exec(qUpsertChunk, ino, idx, buf)
	return err
}

func (tx *Tx) Readlink(ino uint64) (string, error) {
	attr, err := tx.Stat(ino)
	if err != nil {
		return "", err
	}
	if !attr.IsSymlink() {
		return "", ErrInvalid
	}
	return attr.Target, nil
}

func (tx *Tx) GetXattr(ino uint64, name string) ([]byte, error) {
	var value []byte
	err := tx.queryRow(qGetXattr, ino, name).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoXattr
	}
	return value, err
}

func (tx *Tx) SetXattr(ino uint64, name string, value []byte, flags int) error {
	if _, err := tx.Stat(ino); err != nil {
		return err
	}
	_, err := tx.GetXattr(ino, name)
	exists := err == nil
	if err != nil && !errors.Is(err, ErrNoXattr) {
		return err
	}
	const xattrCreate, xattrReplace = 1, 2
	if flags&xattrCreate != 0 && exists {
		return ErrExists
	}
	if flags&xattrReplace != 0 && !exists {
		return ErrNoXattr
	}
	_, err = tx.exec(`INSERT INTO xattrs(ino, name, value) VALUES (?, ?, ?) ON CONFLICT(ino, name) DO UPDATE SET value = excluded.value`, ino, name, value)
	return err
}

func (tx *Tx) RemoveXattr(ino uint64, name string) error {
	res, err := tx.exec(`DELETE FROM xattrs WHERE ino = ? AND name = ?`, ino, name)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNoXattr
	}
	return nil
}

func (tx *Tx) ListXattr(ino uint64) ([]string, error) {
	rows, err := tx.query(`SELECT name FROM xattrs WHERE ino = ? ORDER BY name`, ino)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

type Stats struct {
	Inodes int64
	Bytes  int64
}

func (tx *Tx) Stats() (Stats, error) {
	var s Stats
	if err := tx.queryRow(`SELECT COUNT(*) FROM inodes`).Scan(&s.Inodes); err != nil {
		return s, err
	}
	err := tx.queryRow(`SELECT COALESCE(SUM(size), 0) FROM inodes`).Scan(&s.Bytes)
	return s, err
}

func ModeFromFS(mode fs.FileMode) uint32 {
	out := uint32(mode.Perm())
	switch {
	case mode.IsDir():
		out |= dirMode
	case mode&fs.ModeSymlink != 0:
		out |= symlinkMode
	default:
		out |= regMode
	}
	if mode&fs.ModeSetuid != 0 {
		out |= syscall.S_ISUID
	}
	if mode&fs.ModeSetgid != 0 {
		out |= syscall.S_ISGID
	}
	if mode&fs.ModeSticky != 0 {
		out |= syscall.S_ISVTX
	}
	return out
}

func ModeToFS(mode uint32) fs.FileMode {
	out := fs.FileMode(mode & 0o777)
	switch mode & typeMask {
	case dirMode:
		out |= fs.ModeDir
	case symlinkMode:
		out |= fs.ModeSymlink
	}
	if mode&syscall.S_ISUID != 0 {
		out |= fs.ModeSetuid
	}
	if mode&syscall.S_ISGID != 0 {
		out |= fs.ModeSetgid
	}
	if mode&syscall.S_ISVTX != 0 {
		out |= fs.ModeSticky
	}
	return out
}

func errnoOf(err error) (syscall.Errno, bool) {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno, true
	}
	return 0, false
}

func Errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errno, ok := errnoOf(err); ok {
		return errno
	}
	return syscall.EIO
}
