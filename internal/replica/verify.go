package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// VerifyOptions controls how much data VerifyMetadataWithOptions fetches.
type VerifyOptions struct {
	Deep bool
}

// VerifyReport summarizes a read-only integrity walk over a volume's
// remote metadata. Problems lists every reachability or consistency
// failure found; an empty list means the walked metadata is intact.
type VerifyReport struct {
	Manifests                int
	Bundles                  int
	RefsChecked              int
	ExternalObjects          int
	BytesFetched             int64
	TruncatedBelowCheckpoint bool
	OldestGeneration         uint64
	Problems                 []string
}

// VerifyMetadata walks every manifest, segment reference, and manifest
// bundle reachable from state and reports anything missing, unresolvable,
// or inconsistent. It never writes to the store or the local filesystem.
func (r *Remote) VerifyMetadata(ctx context.Context, state State) (VerifyReport, error) {
	return r.VerifyMetadataWithOptions(ctx, state, VerifyOptions{})
}

// VerifyMetadataWithOptions is VerifyMetadata with control over depth: when
// opts.Deep is set every external segment object is fetched and its content
// compared against the reference hash instead of only checking existence.
func (r *Remote) VerifyMetadataWithOptions(ctx context.Context, state State, opts VerifyOptions) (VerifyReport, error) {
	var report VerifyReport
	history, err := r.loadHistory(ctx, state)
	if err != nil {
		report.Problems = append(report.Problems, fmt.Sprintf("manifest history walk failed: %v", err))
	}
	report.Manifests = len(history)
	if len(history) > 0 {
		oldest := history[len(history)-1].manifest
		report.OldestGeneration = oldest.Generation
		report.TruncatedBelowCheckpoint = oldest.Generation != 1 &&
			!(oldest.Kind == "checkpoint" && oldest.Parent == "")
	}

	sources := newManifestSourceLoader(r.Store, state)
	fetched := map[string]fetchedObject{}
	for _, entry := range history {
		for index, ref := range entry.manifest.Segments {
			report.RefsChecked++
			if problem := r.verifyRef(ctx, sources, opts, fetched, &report, ref); problem != "" {
				report.Problems = append(report.Problems, fmt.Sprintf("manifest %s ref %d: %s", entry.key, index, problem))
			}
			if err := ctx.Err(); err != nil {
				return report, err
			}
		}
	}

	bundleRef := state.ManifestBundle
	for bundleRef != nil {
		bundle, err := r.readManifestBundle(ctx, state, *bundleRef)
		if err != nil {
			report.Problems = append(report.Problems, fmt.Sprintf("manifest bundle %s: %v", bundleRef.Key, err))
			break
		}
		report.Bundles++
		bundleRef = bundle.Parent
		if err := ctx.Err(); err != nil {
			return report, err
		}
	}
	return report, ctx.Err()
}

type fetchedObject struct {
	digest string
	size   int64
	err    error
}

func (r *Remote) verifyRef(
	ctx context.Context,
	sources *manifestSourceLoader,
	opts VerifyOptions,
	fetched map[string]fetchedObject,
	report *VerifyReport,
	ref ObjectRef,
) string {
	switch {
	case ref.Key != "":
		result, seen := fetched[ref.Key]
		if !seen {
			result = r.fetchExternal(ctx, opts, report, ref.Key)
			fetched[ref.Key] = result
		}
		if result.err != nil {
			return result.err.Error()
		}
		if opts.Deep && (result.size != ref.Bytes || result.digest != ref.SHA256) {
			return fmt.Sprintf("segment object %s content does not match its reference", ref.Key)
		}
	case ref.SourceManifest != "":
		if _, _, err := sources.inlineSegment(ctx, ref); err != nil {
			return err.Error()
		}
	}
	return ""
}

func (r *Remote) fetchExternal(ctx context.Context, opts VerifyOptions, report *VerifyReport, key string) fetchedObject {
	obj, err := r.Store.Get(ctx, key)
	if err != nil {
		return fetchedObject{err: fmt.Errorf("segment object %s: %v", key, err)}
	}
	report.BytesFetched += int64(len(obj.Data))
	report.ExternalObjects++
	result := fetchedObject{size: int64(len(obj.Data))}
	if opts.Deep {
		digest := sha256.Sum256(obj.Data)
		result.digest = hex.EncodeToString(digest[:])
	}
	return result
}
