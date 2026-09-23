// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
)

// Manifest rewrite (caver-go#4413): the in-place, metadata-only rebalance.
//
// A table accretes manifests one commit at a time, and each one covers whatever
// partitions that commit touched, so planning a single (index, day) reads many
// manifests and discards most of their entries. RewriteManifests commits ONE
// snapshot (operation "replace") whose manifest set swaps N source data manifests
// for K new ones holding exactly the same live entries, sorted by partition tuple
// and cut at a target entry count, so the per-manifest partition summaries prune.
//
// Nothing else changes: no data file is read, written or removed, every entry
// keeps its snapshot id, data sequence number and file sequence number (written
// EXISTING), delete-file manifests and manifests the filter excludes are carried
// untouched, and the table is never absent from the catalog. In root mode only the
// children are rewritten; the parent's inline section is carried, since it is
// already bounded by the inline maximum and a later commit may be promoting it.
//
// Concurrency. A commit that lands in between leaves the sources in place (an
// append only adds manifests) or replaces one of them (an overwrite rewrote it to
// drop a file). The OCC rebuild handles the first by carrying the new manifests
// next to ours, and refuses the second with ErrCommitDiverged: the entries were
// read from a manifest that no longer exists, so writing them would resurrect
// whatever that commit removed.

// RewriteManifestsTargetEntriesDefault is the default entry count per new manifest:
// about 8 MB of manifest at typical stats widths, a few thousand files.
const RewriteManifestsTargetEntriesDefault = 4000

type rewriteManifestsCfg struct {
	targetEntries int
	keep          func(iceberg.ManifestFile) bool
}

// RewriteManifestsOption tunes one manifest rewrite.
type RewriteManifestsOption func(*rewriteManifestsCfg)

// WithRewriteManifestsTargetEntries sets how many entries each new manifest holds
// before the next one starts. n <= 0 keeps the default.
func WithRewriteManifestsTargetEntries(n int) RewriteManifestsOption {
	return func(c *rewriteManifestsCfg) {
		if n > 0 {
			c.targetEntries = n
		}
	}
}

// WithRewriteManifestsFilter limits the rewrite to the data manifests for which
// rewrite returns true; the rest are carried as they are. Delete-file manifests are
// never rewritten, whatever the filter says.
func WithRewriteManifestsFilter(rewrite func(iceberg.ManifestFile) bool) RewriteManifestsOption {
	return func(c *rewriteManifestsCfg) { c.keep = rewrite }
}

// RewriteManifestsResult reports what a rewrite staged on the transaction.
type RewriteManifestsResult struct {
	// SourceManifests is how many manifests were replaced.
	SourceManifests int
	// NewManifests is how many manifests replaced them.
	NewManifests int
	// KeptManifests is how many of the parent's manifests were carried untouched
	// (delete manifests, filtered-out manifests, and in root mode the inline record).
	KeptManifests int
	// Entries, Rows and Bytes total the live data files moved from the sources to
	// the new manifests; they are identical before and after by construction.
	Entries int64
	Rows    int64
	Bytes   int64
}

// RewriteManifests stages a manifest rewrite on the transaction. With nothing to
// rewrite (no snapshot, or no data manifest selected) it stages nothing and returns
// a zero result, so a caller can run it unconditionally.
func (t *Transaction) RewriteManifests(ctx context.Context, snapshotProps iceberg.Properties, opts ...RewriteManifestsOption) (RewriteManifestsResult, error) {
	var res RewriteManifestsResult
	cfg := rewriteManifestsCfg{targetEntries: RewriteManifestsTargetEntriesDefault}
	for _, o := range opts {
		o(&cfg)
	}
	if t.meta.formatVersion >= 3 {
		return res, fmt.Errorf("%w: manifest rewrite is not supported on format version 3 tables (row lineage needs first_row_id carried per manifest)", ErrInvalidOperation)
	}
	snap := t.meta.currentSnapshot()
	if snap == nil {
		return res, nil
	}
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return res, err
	}
	parent, err := snap.Manifests(fs)
	if err != nil {
		return res, fmt.Errorf("rewrite manifests: read current manifests: %w", err)
	}

	var sources []iceberg.ManifestFile
	for _, m := range parent {
		if iceberg.IsInlineRootManifest(m) || m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		if cfg.keep != nil && !cfg.keep(m) {
			continue
		}
		sources = append(sources, m)
	}
	if len(sources) == 0 {
		return res, nil
	}

	up := t.updateSnapshot(fs, snapshotProps, OpReplace)
	prod := createSnapshotProducer(OpReplace, t, up.io, nil, snapshotProps)
	rw := &rewriteManifests{base: prod, cfg: cfg, sources: make(map[string]struct{}, len(sources))}
	prod.producerImpl = rw
	for _, m := range sources {
		rw.sources[m.FilePath()] = struct{}{}
	}

	written, st, err := rw.writeClustered(sources)
	if err != nil {
		return res, err
	}
	rw.written = written
	if prod.snapshotProps == nil {
		prod.snapshotProps = iceberg.Properties{}
	}
	prod.snapshotProps["manifests-replaced"] = strconv.Itoa(len(sources))
	prod.snapshotProps["manifests-created"] = strconv.Itoa(len(written))
	prod.snapshotProps["manifests-kept"] = strconv.Itoa(len(parent) - len(sources))
	prod.snapshotProps["entries-processed"] = strconv.FormatInt(st.Entries, 10)

	updates, reqs, err := prod.commit(ctx)
	if err != nil {
		return res, err
	}
	if err := t.apply(updates, reqs); err != nil {
		return res, err
	}
	st.SourceManifests = len(sources)
	st.NewManifests = len(written)
	st.KeptManifests = len(parent) - len(sources)

	return st, nil
}

type rewriteManifests struct {
	base    *snapshotProducer
	cfg     rewriteManifestsCfg
	sources map[string]struct{}
	written []iceberg.ManifestFile
}

func (r *rewriteManifests) processManifests(ms []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	return ms, nil
}

// existingManifests is the parent's manifests minus the sources, plus ours.
func (r *rewriteManifests) existingManifests() ([]iceberg.ManifestFile, error) {
	snap, err := r.base.txn.meta.SnapshotByID(r.base.parentSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("rewrite manifests: parent snapshot %d: %w", r.base.parentSnapshotID, err)
	}
	parent, err := snap.Manifests(r.base.io)
	if err != nil {
		return nil, err
	}
	kept, err := r.filterInherited(parent)
	if err != nil {
		return nil, err
	}

	return slices.Concat(r.written, kept), nil
}

// filterInherited drops the sources from a parent's manifest set, and refuses when
// one of them is gone: a concurrent commit rewrote it, and our copy of its entries
// may hold a file that commit removed.
func (r *rewriteManifests) filterInherited(parent []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	seen := 0
	kept := make([]iceberg.ManifestFile, 0, len(parent))
	for _, m := range parent {
		if _, src := r.sources[m.FilePath()]; src {
			seen++

			continue
		}
		kept = append(kept, m)
	}
	if seen != len(r.sources) {
		return nil, fmt.Errorf("%w: rewrite manifests: %d of %d source manifests were replaced by a concurrent commit", ErrCommitDiverged, len(r.sources)-seen, len(r.sources))
	}

	return kept, nil
}

func (r *rewriteManifests) deletedEntries(context.Context) ([]iceberg.ManifestEntry, error) {
	return nil, nil
}

// validate is a no-op: the rebuild's filterInherited is the conflict check, since
// what matters is whether the sources still exist, not which files were added.
func (r *rewriteManifests) validate(*conflictContext) error { return nil }

func (r *rewriteManifests) needsValidation() bool { return false }

// writeClustered reads the live entries of the sources, sorts them by partition
// tuple within each spec, and writes them out in runs of the target size.
func (r *rewriteManifests) writeClustered(sources []iceberg.ManifestFile) ([]iceberg.ManifestFile, RewriteManifestsResult, error) {
	var st RewriteManifestsResult
	bySpec := map[int32][]iceberg.ManifestEntry{}
	for _, m := range sources {
		for e, err := range m.Entries(r.base.io, true) {
			if err != nil {
				return nil, st, fmt.Errorf("rewrite manifests: read %s: %w", m.FilePath(), err)
			}
			bySpec[m.PartitionSpecID()] = append(bySpec[m.PartitionSpecID()], e)
			df := e.DataFile()
			st.Entries++
			st.Rows += df.Count()
			st.Bytes += df.FileSizeBytes()
		}
	}

	specIDs := make([]int32, 0, len(bySpec))
	for id := range bySpec {
		specIDs = append(specIDs, id)
	}
	slices.Sort(specIDs)

	var out []iceberg.ManifestFile
	for _, id := range specIDs {
		spec := r.base.spec(int(id))
		entries := bySpec[id]
		slices.SortStableFunc(entries, func(a, b iceberg.ManifestEntry) int {
			if c := comparePartitions(spec, a.DataFile().Partition(), b.DataFile().Partition()); c != 0 {
				return c
			}

			return strings.Compare(a.DataFile().FilePath(), b.DataFile().FilePath())
		})
		for start := 0; start < len(entries); start += r.cfg.targetEntries {
			end := min(start+r.cfg.targetEntries, len(entries))
			mf, err := r.writeOne(spec, entries[start:end])
			if err != nil {
				return nil, st, err
			}
			out = append(out, mf)
		}
	}

	return out, st, nil
}

func (r *rewriteManifests) writeOne(spec iceberg.PartitionSpec, entries []iceberg.ManifestEntry) (_ iceberg.ManifestFile, err error) {
	wr, path, counter, closer, err := r.base.newManifestWriter(spec, iceberg.WithManifestWriterContent(iceberg.ManifestContentData))
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(closer, &err)
	for _, e := range entries {
		if err := wr.Existing(e); err != nil {
			return nil, fmt.Errorf("rewrite manifests: entry %s: %w", e.DataFile().FilePath(), err)
		}
	}
	if err := wr.Close(); err != nil {
		return nil, err
	}

	return wr.ToManifestFile(path, counter.Count, iceberg.WithManifestFileContent(iceberg.ManifestContentData))
}

// comparePartitions orders two partition tuples field by field in spec order, with
// numbers compared as numbers (a string compare puts day 10 before day 9) and a
// null before any value.
func comparePartitions(spec iceberg.PartitionSpec, a, b map[int]any) int {
	for _, f := range spec.Fields() {
		if c := comparePartitionValue(a[f.FieldID], b[f.FieldID]); c != 0 {
			return c
		}
	}

	return 0
}

func comparePartitionValue(a, b any) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	if x, ok := asInt64(a); ok {
		if y, ok := asInt64(b); ok {
			return cmp.Compare(x, y)
		}
	}
	if x, ok := a.(string); ok {
		if y, ok := b.(string); ok {
			return strings.Compare(x, y)
		}
	}

	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case iceberg.Date:
		return int64(n), true
	case iceberg.Timestamp:
		return int64(n), true
	}

	return 0, false
}
