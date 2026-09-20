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
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
)

// Root-manifest write side (iceberg root_manifest.go; caver-go#4443).
//
// With RootManifestKey="inline" a commit writes ONE file, the root: its records are
// the snapshot's inlined data-file entries (the parent root's inlined entries carried
// as EXISTING, minus any this commit deleted, plus this commit's added data files),
// and its header lists the child manifests (the parent's children, minus any an
// overwrite rewrote, plus any manifests this commit wrote for deletes, delete files,
// or a promotion). When the inlined section would exceed RootManifestInlineMaxKey
// entries it is PROMOTED: sorted by partition tuple and written out as a child
// manifest, and the new root starts with an empty inlined section. That promotion
// is the routine form of the manifest rebalance: it only ever touches the inlined
// section, so its cost is bounded by the inline maximum, never by the table.

const rootManifestInlineMaxDefault = 2000

func (sp *snapshotProducer) rootMode() bool {
	return sp.txn != nil && sp.txn.meta != nil &&
		sp.txn.meta.props.Get(RootManifestKey, "") == RootManifestInline
}

func rootInlineMax(props iceberg.Properties) int {
	if v, err := strconv.Atoi(strings.TrimSpace(props.Get(RootManifestInlineMaxKey, ""))); err == nil && v > 0 {
		return v
	}

	return rootManifestInlineMaxDefault
}

// rootInputs is everything writeRoot needs; the OCC rebuild fills it from the fresh
// parent instead of the one the commit planned against.
type rootInputs struct {
	fio           iceio.WriteFileIO
	parentEntries []iceberg.ManifestFile      // the parent's Manifests(): children plus its inline record
	ownChildren   []iceberg.ManifestFile      // manifests this commit wrote (deletes, delete files, promotions)
	added         []iceberg.DataFile          // data files this commit adds, inlined
	deleted       map[string]iceberg.DataFile // data-file paths this commit removes
	snapshotID    int64
	sequence      int64
	parentID      *int64
	fname         string
}

// splitRoot separates a parent's manifest set into children and the inline record.
func splitRoot(ms []iceberg.ManifestFile) (children []iceberg.ManifestFile, inline iceberg.ManifestFile) {
	for _, m := range ms {
		if iceberg.IsInlineRootManifest(m) {
			inline = m

			continue
		}
		children = append(children, m)
	}

	return children, inline
}

// writeRoot writes the root manifest for one snapshot and returns its path.
func (sp *snapshotProducer) writeRoot(in rootInputs) (_ string, err error) {
	if sp.txn.meta.formatVersion >= 3 {
		return "", fmt.Errorf("%w: root manifest is not yet supported on format version 3 tables (row lineage needs the manifest list's first_row_id accounting)", ErrInvalidOperation)
	}
	spec, err := sp.txn.meta.CurrentSpec()
	if err != nil || spec == nil {
		return "", fmt.Errorf("root manifest: current partition spec: %w", err)
	}
	schema := sp.txn.meta.CurrentSchema()
	base := ""
	if sp.txn.meta.props.Get(RelativePathsKey, "") == iceio.RelativePathsWarehouse {
		base = iceio.PathBaseOf(in.fio)
	}

	parentChildren, parentInline := splitRoot(in.parentEntries)
	children := slices.Concat(parentChildren, in.ownChildren)
	for i, c := range children {
		// Manifests this commit wrote carry no sequence yet; the list writer would
		// assign the commit's sequence, so the root header does the same.
		children[i] = iceberg.AssignRootSequence(c, in.sequence)
	}

	// Inlined entries: the parent's, carried as EXISTING unless deleted here (they
	// keep their own snapshot id and sequence), then ours as ADDED.
	var carried []iceberg.ManifestEntry
	if parentInline != nil {
		for entry, ierr := range parentInline.Entries(in.fio, true) {
			if ierr != nil {
				return "", fmt.Errorf("root manifest: read parent inline entries: %w", ierr)
			}
			if _, gone := in.deleted[entry.DataFile().FilePath()]; gone {
				continue
			}
			carried = append(carried, entry)
		}
	}
	added := make([]iceberg.ManifestEntry, 0, len(in.added))
	for _, df := range in.added {
		added = append(added, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &in.snapshotID, nil, nil, df))
	}

	// Promotion: past the bound, the whole inlined section becomes a child manifest,
	// sorted by partition tuple so a later reader prunes it well.
	if len(carried)+len(added) > rootInlineMax(sp.txn.meta.props) {
		child, perr := sp.writePromotedChild(in, *spec, schema, carried, added, base)
		if perr != nil {
			return "", perr
		}
		children = append(children, iceberg.AssignRootSequence(child, in.sequence))
		carried, added = nil, nil
	}

	// Pass 1: the inline record (counts, partition summaries) for the header.
	inlineRec, err := sp.encodeInline(io.Discard, *spec, schema, carried, added, in.snapshotID, base, nil)
	if err != nil {
		return "", err
	}
	// The inline record's sequence is this commit's: entries written ADDED inherit it
	// when read back, and a later carry writes them EXISTING with it.
	inlineRec = iceberg.AssignRootSequence(inlineRec, in.sequence)
	childrenJSON, err := iceberg.EncodeRootChildren(children, base)
	if err != nil {
		return "", err
	}
	inlineJSON, err := iceberg.EncodeRootInline(inlineRec)
	if err != nil {
		return "", err
	}
	// Pass 2: the root itself.
	provider, err := sp.txn.tbl.LocationProvider()
	if err != nil {
		return "", err
	}
	rootPath := provider.NewMetadataLocation(in.fname)
	out, err := in.fio.Create(rootPath)
	if err != nil {
		return "", fmt.Errorf("root manifest: create %s: %w", rootPath, err)
	}
	defer internal.CheckedClose(out, &err)
	if _, err := sp.encodeInline(out, *spec, schema, carried, added, in.snapshotID, base, map[string][]byte{
		iceberg.RootManifestMarkerKey:   []byte("1"),
		iceberg.RootManifestChildrenKey: childrenJSON,
		iceberg.RootManifestInlineKey:   inlineJSON,
	}); err != nil {
		return "", err
	}

	return rootPath, nil
}

// encodeInline writes the inlined entries as a manifest to out and returns the
// manifest record describing them (path left empty; the caller knows the root path).
func (sp *snapshotProducer) encodeInline(out io.Writer, spec iceberg.PartitionSpec, schema *iceberg.Schema, carried, added []iceberg.ManifestEntry, snapshotID int64, base string, extra map[string][]byte) (iceberg.ManifestFile, error) {
	counter := &internal.CountingWriter{W: out}
	opts := []iceberg.ManifestWriterOption{iceberg.WithManifestWriterContent(iceberg.ManifestContentData), iceberg.WithManifestWriterPathBase(base), iceberg.WithManifestWriterAllowEmpty()}
	if extra != nil {
		opts = append(opts, iceberg.WithManifestWriterMetadata(extra))
	}
	wr, err := iceberg.NewManifestWriter(sp.txn.meta.formatVersion, counter, spec, schema, snapshotID, opts...)
	if err != nil {
		return nil, fmt.Errorf("root manifest: writer: %w", err)
	}
	for _, e := range carried {
		if err := wr.Existing(e); err != nil {
			return nil, fmt.Errorf("root manifest: carried entry %s: %w", e.DataFile().FilePath(), err)
		}
	}
	for _, e := range added {
		if err := wr.Add(e); err != nil {
			return nil, fmt.Errorf("root manifest: added entry %s: %w", e.DataFile().FilePath(), err)
		}
	}
	// The root may legitimately have nothing inlined (everything promoted, or an
	// empty table); WithManifestWriterAllowEmpty lets it close.
	if err := wr.Close(); err != nil {
		return nil, err
	}

	return wr.ToManifestFile("", counter.Count)
}

// writePromotedChild writes the inlined entries as one child manifest, sorted by
// partition tuple, and returns its record.
func (sp *snapshotProducer) writePromotedChild(in rootInputs, spec iceberg.PartitionSpec, schema *iceberg.Schema, carried, added []iceberg.ManifestEntry, base string) (_ iceberg.ManifestFile, err error) {
	isAdded := make(map[iceberg.ManifestEntry]struct{}, len(added))
	for _, e := range added {
		isAdded[e] = struct{}{}
	}
	entries := slices.Concat(carried, added)
	slices.SortStableFunc(entries, func(a, b iceberg.ManifestEntry) int {
		return strings.Compare(partitionKey(a), partitionKey(b))
	})
	provider, err := sp.txn.tbl.LocationProvider()
	if err != nil {
		return nil, err
	}
	fname := newManifestFileName(int(sp.manifestCount.Add(1)), sp.commitUuid)
	path := provider.NewMetadataLocation(fname)
	out, err := in.fio.Create(path)
	if err != nil {
		return nil, fmt.Errorf("root manifest: create promoted child %s: %w", path, err)
	}
	defer internal.CheckedClose(out, &err)
	counter := &internal.CountingWriter{W: out}
	wr, err := iceberg.NewManifestWriter(sp.txn.meta.formatVersion, counter, spec, schema, in.snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentData), iceberg.WithManifestWriterPathBase(base))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		// Carried entries are EXISTING in the child (their own snapshot id and
		// sequence come with them); this commit's own files are ADDED.
		var werr error
		if _, ours := isAdded[e]; ours {
			werr = wr.Add(e)
		} else {
			werr = wr.Existing(e)
		}
		if werr != nil {
			return nil, werr
		}
	}
	if err := wr.Close(); err != nil {
		return nil, err
	}

	return wr.ToManifestFile(path, counter.Count)
}

// partitionKey renders an entry's partition tuple for sorting.
func partitionKey(e iceberg.ManifestEntry) string {
	part := e.DataFile().Partition()
	keys := make([]int, 0, len(part))
	for k := range part {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%d=%v|", k, part[k])
	}

	return b.String()
}
