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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// Snapshot offloading (caver-go#4442): metadata.json keeps the head, a segment chain
// keeps the history, loading merges them back, the chain folds, expiry never
// resurrects, and the orphan sweep leaves the chain alone.

// offloadTable builds a table on LocalFS with n committed snapshots, each commit
// going through the real commit path (AddDataFiles + Commit) so the segment chain is
// written by the code under test.
func offloadTable(t *testing.T, props iceberg.Properties, n int) (*Table, string, iceio.IO) {
	t.Helper()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	if props == nil {
		props = iceberg.Properties{}
	}
	props[PropertyFormatVersion] = "2"
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, warehouse+"/t", props)
	require.NoError(t, err)
	fsys := iceio.WrapRelative(iceio.LocalFS{}, warehouse)
	cat := &offloadCatalog{fs: fsys.(iceio.WriteFileIO), metadata: meta, loc: warehouse + "/t/metadata/v1.metadata.json"}
	require.NoError(t, writeOffloadMetadata(cat))
	// Load the way a catalog does (NewFromLocation merges the chain and keeps the
	// pointer), so every commit below appends to the chain instead of starting one.
	tbl, err := cat.LoadTable(context.Background(), Identifier{"db", "off"})
	require.NoError(t, err)
	cat.metadata = tbl.Metadata()
	for i := 0; i < n; i++ {
		tbl = commitOneFile(t, tbl, warehouse+"/t/data/f"+string(rune('a'+i))+".parquet")
	}

	return tbl, warehouse, fsys
}

// offloadCatalog writes metadata.json through the same path the sql catalog uses
// (WriteTableMetadata), so offloading is exercised end to end, and reloads through
// NewFromLocation so merging is too.
type offloadCatalog struct {
	fs       iceio.WriteFileIO
	metadata Metadata
	loc      string
	version  int
}

func writeOffloadMetadata(c *offloadCatalog) error {
	return writeTableMetadataForTest(c.metadata, c.fs, c.loc)
}

func (c *offloadCatalog) LoadTable(ctx context.Context, ident Identifier) (*Table, error) {
	return NewFromLocation(ctx, ident, c.loc, func(context.Context) (iceio.IO, error) { return c.fs, nil }, c)
}

func (c *offloadCatalog) CommitTable(ctx context.Context, ident Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	meta, err := UpdateTableMetadata(c.metadata, updates, c.loc)
	if err != nil {
		return nil, "", err
	}
	c.version++
	c.loc = strings.TrimSuffix(c.loc[:strings.LastIndex(c.loc, "/")], "") + "/v" + string(rune('2'+c.version-1)) + ".metadata.json"
	if err := writeTableMetadataForTest(meta, c.fs, c.loc); err != nil {
		return nil, "", err
	}
	// What a catalog hands back is what a reader would load: reload from disk so the
	// returned Metadata reflects the offloaded file plus the merged chain.
	reloaded, err := c.LoadTable(ctx, ident)
	if err != nil {
		return nil, "", err
	}
	c.metadata = reloaded.Metadata()

	return c.metadata, c.loc, nil
}

// onDisk decodes metadata.json WITHOUT merging: what a standard reader sees.
func onDisk(t *testing.T, loc string) *commonMetadata {
	t.Helper()
	b, err := os.ReadFile(strings.TrimPrefix(loc, "file://"))
	require.NoError(t, err)
	m, err := ParseMetadataBytes(b)
	require.NoError(t, err)
	c, ok := commonOf(m)
	require.True(t, ok)

	return c
}

func segmentDepth(t *testing.T, fsys iceio.IO, m Metadata) int {
	t.Helper()
	segs, err := SegmentChainPaths(m, fsys)
	require.NoError(t, err)

	return len(segs)
}

func TestSnapshotOffloadingKeepsHeadInlineAndHistoryInSegments(t *testing.T) {
	tbl, _, fsys := offloadTable(t, iceberg.Properties{SnapshotsOffloadedKey: SnapshotsOffloadedValue}, 3)
	cat := tbl.cat.(*offloadCatalog)

	// In memory: full history.
	require.Len(t, tbl.Metadata().Snapshots(), 3)
	// On disk: the head only, plus the pointer.
	disk := onDisk(t, cat.loc)
	require.Len(t, disk.SnapshotList, 1, "metadata.json must carry only the head snapshot")
	require.Equal(t, *disk.CurrentSnapshotID, disk.SnapshotList[0].SnapshotID)
	require.NotEmpty(t, disk.SnapshotsFile)
	require.True(t, iceio.IsRelativePath(disk.SnapshotsFile), "pointer is warehouse-relative on a based FileIO: %q", disk.SnapshotsFile)
	require.Len(t, disk.SnapshotLog, 1)
	// One segment per write, chained: the empty one from the create plus one per commit.
	require.Equal(t, 4, segmentDepth(t, fsys, tbl.Metadata()))

	// A fresh load merges the chain back: same ids, same order, log restored.
	reloaded, err := cat.LoadTable(context.Background(), Identifier{"db", "off"})
	require.NoError(t, err)
	require.Len(t, reloaded.Metadata().Snapshots(), 3)
	require.Equal(t, tbl.Metadata().Snapshots(), reloaded.Metadata().Snapshots())
	logs := 0
	for range reloaded.Metadata().SnapshotLogs() {
		logs++
	}
	require.Equal(t, 3, logs)
	// Time travel by id works off the merged list.
	first := tbl.Metadata().Snapshots()[0].SnapshotID
	require.NotNil(t, reloaded.Metadata().SnapshotByID(first))

	// A standard reader (no merge) sees a table whose history starts at the head.
	std, err := ParseMetadataBytes(mustReadFile(t, cat.loc))
	require.NoError(t, err)
	require.Len(t, std.Snapshots(), 1)
	require.NotNil(t, std.CurrentSnapshot())
}

func TestSnapshotOffloadingFoldsTheChain(t *testing.T) {
	tbl, _, fsys := offloadTable(t, iceberg.Properties{SnapshotsOffloadedKey: SnapshotsOffloadedValue, SnapshotsFoldEveryKey: "2"}, 3)
	// Writes: create -> depth 1; commit 1 -> 2; commit 2 sees 2 >= fold-every and
	// folds -> 1; commit 3 -> 2. Without folding the depth would be 4.
	require.Equal(t, 2, segmentDepth(t, fsys, tbl.Metadata()))
	require.Len(t, tbl.Metadata().Snapshots(), 3)
	segs, err := SegmentChainPaths(tbl.Metadata(), fsys)
	require.NoError(t, err)
	var head, root snapshotSegment
	require.NoError(t, json.Unmarshal(mustReadFile(t, segs[0]), &head))
	require.NoError(t, json.Unmarshal(mustReadFile(t, segs[1]), &root))
	require.NotEmpty(t, head.Parent)
	require.Len(t, head.Snapshots, 1, "a chained segment holds only what its commit added")
	require.Empty(t, root.Parent)
	require.Len(t, root.Snapshots, 2, "a folded segment holds the whole live list at fold time")
}

func TestSnapshotOffloadingExpiryNeverResurrects(t *testing.T) {
	tbl, _, fsys := offloadTable(t, iceberg.Properties{SnapshotsOffloadedKey: SnapshotsOffloadedValue}, 3)
	cat := tbl.cat.(*offloadCatalog)
	ctx := context.Background()
	snaps := tbl.Metadata().Snapshots()
	oldest := snaps[0].SnapshotID

	// Expire the oldest through the builder, the way ExpireSnapshots does.
	b, err := MetadataBuilderFromBase(tbl.Metadata(), cat.loc)
	require.NoError(t, err)
	require.NoError(t, b.RemoveSnapshots([]int64{oldest}, false))
	meta, err := b.Build()
	require.NoError(t, err)
	cat.metadata = meta
	cat.loc = strings.Replace(cat.loc, ".metadata.json", "x.metadata.json", 1)
	require.NoError(t, writeOffloadMetadata(cat))

	reloaded, err := cat.LoadTable(ctx, Identifier{"db", "off"})
	require.NoError(t, err)
	require.Len(t, reloaded.Metadata().Snapshots(), 2)
	require.Nil(t, reloaded.Metadata().SnapshotByID(oldest), "an expired snapshot must not come back through the chain")
	// The chain folded: one segment, no parent, exactly the live two.
	require.Equal(t, 1, segmentDepth(t, fsys, reloaded.Metadata()))
}

func TestSnapshotOffloadingOffWritesStandardMetadata(t *testing.T) {
	tbl, _, fsys := offloadTable(t, nil, 2)
	cat := tbl.cat.(*offloadCatalog)
	disk := onDisk(t, cat.loc)
	require.Len(t, disk.SnapshotList, 2)
	require.Empty(t, disk.SnapshotsFile)
	require.Equal(t, 0, segmentDepth(t, fsys, tbl.Metadata()))
}

func TestSnapshotOffloadingChainSurvivesTheOrphanSweep(t *testing.T) {
	tbl, warehouse, _ := offloadTable(t, iceberg.Properties{SnapshotsOffloadedKey: SnapshotsOffloadedValue}, 3)
	ctx := context.Background()
	dir := strings.TrimPrefix(warehouse, "file://")
	require.NoError(t, os.WriteFile(dir+"/t/metadata/stray-segment.json", []byte("{}"), 0o644))
	res, err := tbl.DeleteOrphanFiles(ctx, WithDryRun(true), WithFilesOlderThan(0), WithEqualSchemes(map[string]string{"file": ""}))
	require.NoError(t, err)
	for _, p := range res.OrphanFileLocations {
		require.NotContains(t, p, "snapshots-", "a live segment must never be an orphan candidate: %v", res.OrphanFileLocations)
	}
	found := false
	for _, p := range res.OrphanFileLocations {
		if strings.Contains(p, "stray-segment.json") {
			found = true
		}
	}
	require.True(t, found, "the planted stray must still be found: %v", res.OrphanFileLocations)
}

func TestInlineSnapshotsGivesTheStandardForm(t *testing.T) {
	tbl, _, _ := offloadTable(t, iceberg.Properties{SnapshotsOffloadedKey: SnapshotsOffloadedValue}, 2)
	std, err := InlineSnapshots(tbl.Metadata())
	require.NoError(t, err)
	require.Len(t, std.Snapshots(), 2)
	c, ok := commonOf(std)
	require.True(t, ok)
	require.Empty(t, c.SnapshotsFile)
	// And the original is untouched.
	require.NotEmpty(t, offloadedFile(tbl.Metadata()))
}

func mustReadFile(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(strings.TrimPrefix(p, "file://"))
	require.NoError(t, err)

	return b
}
