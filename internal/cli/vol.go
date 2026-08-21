package cli

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/zephyraoss/satchel/internal/litestream"
	"github.com/zephyraoss/satchel/internal/store"
)

type EnvFactory func(ctx context.Context) (*Env, error)

func NewVolCommand(factory EnvFactory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "vol",
		Short: "Inspect and edit volumes directly in the bucket (no node access needed)",
	}
	cmd.AddCommand(
		newLsCommand(factory),
		newLsFilesCommand(factory),
		newCatCommand(factory),
		newPutCommand(factory),
		newEditCommand(factory),
		newSQLCommand(factory),
		newRestoreCommand(factory),
		newLeaseCommand(factory),
	)
	return cmd
}

func newLsCommand(factory EnvFactory) *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List volumes in the bucket with their lease state",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			return e.ls(cmd.Context())
		},
	}
}

func (e *Env) ls(ctx context.Context) error {
	keys, err := e.Store.List(ctx, "vols/")
	if err != nil {
		return err
	}
	counts := map[string]int{}
	for _, key := range keys {
		rest := strings.TrimPrefix(key, "vols/")
		if i := strings.Index(rest, "/"); i > 0 {
			counts[rest[:i]]++
		}
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	tw := tabwriter.NewWriter(e.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "VOLUME\tOBJECTS\tLEASE\tEXPIRES")
	for _, name := range names {
		holder, expires := "-", "-"
		if rec, err := e.Leases.Inspect(ctx, name); err == nil && rec != nil {
			holder = rec.Holder
			if rec.ExpiresAt.Before(time.Now()) {
				holder += " (expired)"
			}
			expires = formatTime(rec.ExpiresAt)
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", name, counts[name], holder, expires)
	}
	return tw.Flush()
}

func newLsFilesCommand(factory EnvFactory) *cobra.Command {
	var recursive bool
	cmd := &cobra.Command{
		Use:   "ls-files <volume> [path]",
		Short: "List files inside a volume",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			p := ""
			if len(args) == 2 {
				p = args[1]
			}
			return e.lsFiles(cmd.Context(), args[0], p, recursive)
		},
	}
	cmd.Flags().BoolVarP(&recursive, "recursive", "r", false, "descend into subdirectories")
	return cmd
}

func (e *Env) lsFiles(ctx context.Context, volume, p string, recursive bool) error {
	return e.withSnapshot(ctx, volume, litestream.RestoreOptions{}, func(db *store.DB) error {
		return db.View(ctx, func(tx *store.Tx) error {
			root, err := resolve(tx, p)
			if err != nil {
				return err
			}
			tw := tabwriter.NewWriter(e.Stdout, 0, 4, 2, ' ', 0)
			defer tw.Flush()
			if !root.IsDir() {
				printEntry(tw, strings.Trim(p, "/"), root)
				return nil
			}
			return walkDir(tx, root.Ino, strings.Trim(p, "/"), recursive, func(path string, attr store.Attr) {
				printEntry(tw, path, attr)
			})
		})
	})
}

func walkDir(tx *store.Tx, ino uint64, prefix string, recursive bool, fn func(string, store.Attr)) error {
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
		fn(path, attr)
		if recursive && attr.IsDir() {
			if err := walkDir(tx, entry.Ino, path, true, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func printEntry(w io.Writer, path string, attr store.Attr) {
	name := path
	if attr.IsSymlink() {
		name += " -> " + attr.Target
	}
	if attr.IsDir() {
		name += "/"
	}
	fmt.Fprintf(w, "%s\t%d\t%d:%d\t%s\t%s\n", store.ModeToFS(attr.Mode), attr.Size, attr.Uid, attr.Gid, formatTime(attr.Mtime), name)
}

func newCatCommand(factory EnvFactory) *cobra.Command {
	return &cobra.Command{
		Use:   "cat <volume> <path>",
		Short: "Print a file from a volume",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			return e.Cat(cmd.Context(), args[0], args[1], e.Stdout)
		},
	}
}

func (e *Env) Cat(ctx context.Context, volume, p string, w io.Writer) error {
	return e.withSnapshot(ctx, volume, litestream.RestoreOptions{}, func(db *store.DB) error {
		return db.View(ctx, func(tx *store.Tx) error {
			attr, err := resolve(tx, p)
			if err != nil {
				return err
			}
			if attr.IsDir() {
				return fmt.Errorf("%s is a directory", p)
			}
			return copyOut(tx, attr, w)
		})
	})
}

func copyOut(tx *store.Tx, attr store.Attr, w io.Writer) error {
	buf := make([]byte, 1<<20)
	for off := int64(0); off < attr.Size; {
		n, err := tx.ReadAt(attr.Ino, buf, off)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
		off += int64(n)
	}
	return nil
}

func newPutCommand(factory EnvFactory) *cobra.Command {
	var mode uint32
	cmd := &cobra.Command{
		Use:   "put <volume> <local-file> <path>",
		Short: "Write a local file into a volume (use - for stdin)",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			var src io.Reader = os.Stdin
			if args[1] != "-" {
				f, err := os.Open(args[1])
				if err != nil {
					return err
				}
				defer f.Close()
				src = f
			}
			return e.Put(cmd.Context(), args[0], args[2], src, mode)
		},
	}
	cmd.Flags().Uint32Var(&mode, "mode", 0o644, "permission bits for a newly created file (octal, e.g. 0600)")
	return cmd
}

func (e *Env) Put(ctx context.Context, volume, p string, src io.Reader, mode uint32) error {
	return e.withWriteLease(ctx, volume, func(db *store.DB) error {
		return db.Do(ctx, func(tx *store.Tx) error {
			return writeFile(tx, p, src, mode)
		})
	})
}

func writeFile(tx *store.Tx, p string, src io.Reader, mode uint32) error {
	dir, base := splitPath(p)
	if base == "" {
		return fmt.Errorf("path %q has no file name", p)
	}
	parent, err := tx.EnsureDir(dir)
	if err != nil {
		return err
	}
	attr, err := tx.Lookup(parent, base)
	switch {
	case err == nil:
		if attr.IsDir() {
			return fmt.Errorf("%s is a directory", p)
		}
		zero := int64(0)
		if _, err := tx.SetAttr(attr.Ino, store.AttrChange{Size: &zero}); err != nil {
			return err
		}
	default:
		attr, err = tx.Create(parent, base, store.NewInode{Mode: store.ModeFromFS(os.FileMode(mode).Perm())})
		if err != nil {
			return err
		}
	}
	return tx.WriteFrom(attr.Ino, src)
}

func newEditCommand(factory EnvFactory) *cobra.Command {
	return &cobra.Command{
		Use:   "edit <volume> <path>",
		Short: "Edit a file in a volume with $EDITOR (holds the lease while you edit)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			return e.edit(cmd.Context(), args[0], args[1])
		},
	}
}

func (e *Env) edit(ctx context.Context, volume, p string) error {
	editor := os.Getenv("VISUAL")
	if editor == "" {
		editor = os.Getenv("EDITOR")
	}
	if editor == "" {
		editor = "vi"
	}
	return e.withWriteLease(ctx, volume, func(db *store.DB) error {
		tmp, err := os.CreateTemp(e.WorkDir, "satchel-edit-*"+filepath.Ext(p))
		if err != nil {
			return err
		}
		defer os.Remove(tmp.Name())
		var existing *store.Attr
		err = db.View(ctx, func(tx *store.Tx) error {
			attr, err := resolve(tx, p)
			if err != nil {
				return nil
			}
			existing = &attr
			return copyOut(tx, attr, tmp)
		})
		tmp.Close()
		if err != nil {
			return err
		}
		before, _ := os.Stat(tmp.Name())
		ed := exec.CommandContext(ctx, "sh", "-c", editor+" \"$1\"", "editor", tmp.Name())
		ed.Stdin, ed.Stdout, ed.Stderr = os.Stdin, os.Stdout, os.Stderr
		if err := ed.Run(); err != nil {
			return fmt.Errorf("editor: %w", err)
		}
		after, err := os.Stat(tmp.Name())
		if err != nil {
			return err
		}
		if existing != nil && after.ModTime().Equal(before.ModTime()) && after.Size() == before.Size() {
			fmt.Fprintln(e.Stderr, "unchanged, nothing written")
			return nil
		}
		f, err := os.Open(tmp.Name())
		if err != nil {
			return err
		}
		defer f.Close()
		return db.Do(ctx, func(tx *store.Tx) error {
			return writeFile(tx, p, f, 0o644)
		})
	})
}

func newSQLCommand(factory EnvFactory) *cobra.Command {
	var write bool
	cmd := &cobra.Command{
		Use:   "sql <volume> <statement>",
		Short: "Run SQL against a volume's database (read-only unless --write)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			return e.sql(cmd.Context(), args[0], args[1], write)
		},
	}
	cmd.Flags().BoolVar(&write, "write", false, "take the lease and replicate the result back to the bucket")
	return cmd
}

func (e *Env) sql(ctx context.Context, volume, statement string, write bool) error {
	run := func(db *store.DB) error {
		rows, err := db.QueryContext(ctx, statement)
		if err != nil {
			return err
		}
		defer rows.Close()
		return printRows(e.Stdout, rows)
	}
	if write {
		return e.withWriteLease(ctx, volume, run)
	}
	return e.withSnapshot(ctx, volume, litestream.RestoreOptions{}, run)
}

func printRows(w io.Writer, rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	if len(cols) > 0 {
		fmt.Fprintln(bw, strings.Join(cols, "\t"))
	}
	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		fields := make([]string, len(cols))
		for i, v := range values {
			fields[i] = formatValue(v)
		}
		fmt.Fprintln(bw, strings.Join(fields, "\t"))
	}
	return rows.Err()
}

func formatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		if len(x) > 64 {
			return fmt.Sprintf("<blob %d bytes>", len(x))
		}
		return fmt.Sprintf("%q", x)
	default:
		return fmt.Sprint(x)
	}
}

func newRestoreCommand(factory EnvFactory) *cobra.Command {
	var timestamp, txid string
	cmd := &cobra.Command{
		Use:   "restore <volume> <local-dir>",
		Short: "Export a volume (optionally at a point in time) into a local directory",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			opts := litestream.RestoreOptions{TXID: txid}
			if timestamp != "" {
				t, err := time.Parse(time.RFC3339, timestamp)
				if err != nil {
					return fmt.Errorf("--timestamp must be RFC3339: %w", err)
				}
				opts.Timestamp = t
			}
			return e.restore(cmd.Context(), args[0], args[1], opts)
		},
	}
	cmd.Flags().StringVar(&timestamp, "timestamp", "", "restore the state as of this RFC3339 time")
	cmd.Flags().StringVar(&txid, "txid", "", "restore up to this hex Litestream TXID")
	return cmd
}

func (e *Env) restore(ctx context.Context, volume, dir string, opts litestream.RestoreOptions) error {
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return fmt.Errorf("%s is not empty", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return e.withSnapshot(ctx, volume, opts, func(db *store.DB) error {
		if err := db.Unpack(ctx, dir); err != nil {
			return err
		}
		entries, err := db.ListFiles(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(e.Stderr, "restored %d entries from %s into %s\n", len(entries), volume, dir)
		return nil
	})
}

func newLeaseCommand(factory EnvFactory) *cobra.Command {
	cmd := &cobra.Command{Use: "lease", Short: "Inspect or break a volume's lease"}
	cmd.AddCommand(&cobra.Command{
		Use:   "status <volume>",
		Short: "Show who holds the lease",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			return e.leaseStatus(cmd.Context(), args[0])
		},
	})
	var yes bool
	breakCmd := &cobra.Command{
		Use:   "break <volume>",
		Short: "Delete the lease so another node can mount the volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := factory(cmd.Context())
			if err != nil {
				return err
			}
			return e.leaseBreak(cmd.Context(), args[0], yes)
		},
	}
	breakCmd.Flags().BoolVar(&yes, "yes", false, "skip the confirmation prompt")
	cmd.AddCommand(breakCmd)
	return cmd
}

func (e *Env) leaseStatus(ctx context.Context, volume string) error {
	rec, err := e.Leases.Inspect(ctx, volume)
	if err != nil {
		return err
	}
	if rec == nil {
		fmt.Fprintf(e.Stdout, "%s: not held\n", volume)
		return nil
	}
	state := "held"
	if rec.ExpiresAt.Before(time.Now()) {
		state = "expired"
	}
	fmt.Fprintf(e.Stdout, "%s: %s by %s\n  mounted_at: %s\n  expires_at: %s (%s)\n  token:      %s\n",
		volume, state, rec.Holder, formatTime(rec.MountedAt), formatTime(rec.ExpiresAt), time.Until(rec.ExpiresAt).Round(time.Second), rec.Token)
	return nil
}

func (e *Env) leaseBreak(ctx context.Context, volume string, yes bool) error {
	rec, err := e.Leases.Inspect(ctx, volume)
	if err != nil {
		return err
	}
	if rec == nil {
		fmt.Fprintf(e.Stdout, "%s: no lease to break\n", volume)
		return nil
	}
	fmt.Fprintf(e.Stderr, `
!!! DANGER !!!
Lease on %q is held by %s until %s.

Breaking it lets another node mount the volume while %s may still be running.
If that node is alive, BOTH will write to one Litestream lineage and the
volume WILL be corrupted. Only continue if you are certain the holder is dead
(powered off, container gone, satchel stopped).

`, volume, rec.Holder, formatTime(rec.ExpiresAt), rec.Holder)
	if !yes {
		fmt.Fprintf(e.Stderr, "Type the holder's name (%s) to confirm: ", rec.Holder)
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if strings.TrimSpace(line) != rec.Holder {
			return fmt.Errorf("confirmation did not match; lease left intact")
		}
	}
	if err := e.Leases.Break(ctx, volume); err != nil {
		return err
	}
	fmt.Fprintf(e.Stdout, "lease on %s broken (was held by %s)\n", volume, rec.Holder)
	return nil
}
