package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
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
	fetched := map[string]error{}
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

func (r *Remote) verifyRef(
	ctx context.Context,
	sources *manifestSourceLoader,
	opts VerifyOptions,
	fetched map[string]error,
	report *VerifyReport,
	ref ObjectRef,
) string {
	switch {
	case ref.Key != "":
		result, seen := fetched[ref.Key]
		if !seen {
			result = r.verifyExternal(ctx, opts, report, ref)
			fetched[ref.Key] = result
		}
		if result != nil {
			return result.Error()
		}
	case ref.SourceManifest != "":
		if err := verifySourceRef(ctx, sources, ref); err != nil {
			return err.Error()
		}
	}
	return ""
}

func (r *Remote) verifyExternal(ctx context.Context, opts VerifyOptions, report *VerifyReport, ref ObjectRef) error {
	obj, err := r.Store.Get(ctx, ref.Key)
	if err != nil {
		return fmt.Errorf("segment object %s: %v", ref.Key, err)
	}
	report.BytesFetched += int64(len(obj.Data))
	if opts.Deep {
		digest := sha256.Sum256(obj.Data)
		if int64(len(obj.Data)) != ref.Bytes || hex.EncodeToString(digest[:]) != ref.SHA256 {
			return fmt.Errorf("segment object %s content does not match its reference", ref.Key)
		}
	}
	report.ExternalObjects++
	return nil
}

func verifySourceRef(ctx context.Context, sources *manifestSourceLoader, ref ObjectRef) error {
	var body []byte
	var err error
	if ref.SourceBundle != "" {
		body, err = sources.manifestBody(ctx, ref.SourceBundle, ref.SourceManifest)
	} else {
		body, err = sources.resolve(ctx, ref.SourceManifest)
	}
	if err != nil {
		return fmt.Errorf("source manifest %s is unresolvable: %v", ref.SourceManifest, err)
	}
	digest := sha256.Sum256(body)
	if !strings.HasSuffix(ref.SourceManifest, "/manifests/"+hex.EncodeToString(digest[:])+".json") {
		return fmt.Errorf("source manifest %s checksum mismatch", ref.SourceManifest)
	}
	var source Manifest
	if err := json.Unmarshal(body, &source); err != nil {
		return fmt.Errorf("decode source manifest %s: %v", ref.SourceManifest, err)
	}
	if int(ref.SourceIndex) >= len(source.Segments) {
		return fmt.Errorf("source index %d is outside manifest %s", ref.SourceIndex, ref.SourceManifest)
	}
	sourceRef := source.Segments[ref.SourceIndex]
	if len(sourceRef.InlineData) == 0 || sourceRef.SHA256 != ref.SHA256 ||
		sourceRef.Bytes != ref.Bytes || sourceRef.Blocks != ref.Blocks {
		return fmt.Errorf("inline segment %d in manifest %s does not match its reference", ref.SourceIndex, ref.SourceManifest)
	}
	return nil
}
