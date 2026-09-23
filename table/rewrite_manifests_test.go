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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// Manifest rewrite (caver-go#4413): same live entries, fewer manifests, each one
// covering a tight partition range, the table never absent, and a concurrent
// commit either carried or refused, never duplicated.

type rmFixture struct {
	t         *testing.T
	warehouse string
	spec      iceberg.PartitionSpec
	tbl       *Table
	cat       *sequentialCatalog
}

func newRewriteManifestsFixture(t *testing.T, props iceberg.Properties) *rmFixture {
	t.Helper()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	if props == nil {
		props = iceberg.Properties{}
	}
	props[PropertyFormatVersion] = "2"
	spec := partitionedSpec()
	meta, err := NewMetadata(simpleSchema(), &spec, UnsortedSortOrder, warehouse+"/t", props)
	require.NoError(t, err)
	cat := &sequentialCatalog{metadata: meta}
	fsys := iceio.WrapRelative(iceio.LocalFS{}, warehouse)
	tbl := New(Identifier{"db", "rm"}, meta, warehouse+"/t/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return fsys, nil }, cat)

	return &rmFixture{t: t, warehouse: warehouse, spec: spec, tbl: tbl, cat: cat}
}

func (f *rmFixture) dataFile(name string, part int32) iceberg.DataFile {
	f.t.Helper()
	path := f.warehouse + "/t/data/" + name
	fsys, err := f.tbl.FS(context.Background())
	require.NoError(f.t, err)
	require.NoError(f.t, fsys.(iceio.WriteFileIO).WriteFile(path, []byte("x")))

	return newTestDataFileWithCount(f.t, f.spec, path, map[int]any{1000: part}, int64(part)+1)
}

// commit appends one file per commit, so one manifest per commit in list mode.
func (f *rmFixture) commit(tbl *Table, name string, part int32) *Table {
	f.t.Helper()
	txn := tbl.NewTransaction()
	require.NoError(f.t, txn.AddDataFiles(context.Background(), []iceberg.DataFile{f.dataFile(name, part)}, nil))
	out, err := txn.Commit(context.Background())
	require.NoError(f.t, err)

	return out
}

type liveEntry struct {
	snapshotID int64
	seq        int64
	fileSeq    int64
}

// live maps each live data file to its entry identity, from the manifests the
// current snapshot references.
func live(t *testing.T, tbl *Table) (map[string]liveEntry, []iceberg.ManifestFile) {
	t.Helper()
	fsys, err := tbl.FS(context.Background())
	require.NoError(t, err)
	ms, err := tbl.CurrentSnapshot().Manifests(fsys)
	require.NoError(t, err)
	out := map[string]liveEntry{}
	for _, m := range ms {
		entries, err := m.FetchEntries(fsys, true)
		require.NoError(t, err)
		for _, e := range entries {
			p := filepath.Base(e.DataFile().FilePath())
			_, dup := out[p]
			require.False(t, dup, "%s is live in two manifests", p)
			var fs int64 = -1
			if e.FileSequenceNum() != nil {
				fs = *e.FileSequenceNum()
			}
			out[p] = liveEntry{snapshotID: e.SnapshotID(), seq: e.SequenceNum(), fileSeq: fs}
		}
	}

	return out, ms
}

func plannedFiles(t *testing.T, tbl *Table) []string {
	t.Helper()
	tasks, err := tbl.Scan().PlanFiles(context.Background())
	require.NoError(t, err)
	out := make([]string, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, filepath.Base(task.File.FilePath()))
	}

	return out
}

// interleave commits 12 files across partitions 9, 10, 11 in round-robin, so each
// commit's manifest is one file and the partitions are smeared across manifests.
// 9/10/11 also catch a string sort, which would put 10 and 11 before 9.
func (f *rmFixture) interleave(tbl *Table) *Table {
	for i := 0; i < 12; i++ {
		tbl = f.commit(tbl, fmt.Sprintf("f%02d.parquet", i), int32(9+i%3))
	}

	return tbl
}

func (f *rmFixture) rewrite(tbl *Table, opts ...RewriteManifestsOption) (*Table, RewriteManifestsResult) {
	f.t.Helper()
	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(context.Background(), nil, opts...)
	require.NoError(f.t, err)
	out, err := txn.Commit(context.Background())
	require.NoError(f.t, err)

	return out, res
}

func TestRewriteManifestsClustersAndPreservesEntries(t *testing.T) {
	f := newRewriteManifestsFixture(t, nil)
	tbl := f.interleave(f.tbl)
	before, ms := live(t, tbl)
	require.Len(t, ms, 12, "one manifest per commit")
	totalsBefore := tbl.CurrentSnapshot().Summary.Properties

	tbl, res := f.rewrite(tbl, WithRewriteManifestsTargetEntries(2))
	require.Equal(t, 12, res.SourceManifests)
	require.Equal(t, 6, res.NewManifests, "12 entries at 2 per manifest")
	require.Equal(t, int64(12), res.Entries)

	after, ms := live(t, tbl)
	require.Equal(t, before, after, "every live file keeps its snapshot id, data sequence and file sequence")
	require.ElementsMatch(t, []string{"f00.parquet", "f01.parquet", "f02.parquet", "f03.parquet", "f04.parquet", "f05.parquet",
		"f06.parquet", "f07.parquet", "f08.parquet", "f09.parquet", "f10.parquet", "f11.parquet"}, plannedFiles(t, tbl))

	require.Len(t, ms, 6)
	var lowers []int32
	for _, m := range ms {
		require.Equal(t, int32(2), m.ExistingDataFiles(), "rewritten entries are EXISTING")
		require.Zero(t, m.AddedDataFiles())
		parts := m.Partitions()
		require.Len(t, parts, 1)
		lo, hi := *parts[0].LowerBound, *parts[0].UpperBound
		require.Equal(t, lo, hi, "each manifest covers exactly one partition value")
		v, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Int32, lo)
		require.NoError(t, err)
		lowers = append(lowers, v.(iceberg.Int32Literal).Value())
	}
	require.Equal(t, []int32{9, 9, 10, 10, 11, 11}, lowers, "sorted numerically, not as strings")

	snap := tbl.CurrentSnapshot()
	require.Equal(t, OpReplace, snap.Summary.Operation)
	require.Equal(t, "12", snap.Summary.Properties["manifests-replaced"])
	require.Equal(t, "6", snap.Summary.Properties["manifests-created"])
	for _, k := range []string{"total-data-files", "total-records", "total-files-size"} {
		require.Equal(t, totalsBefore[k], snap.Summary.Properties[k], k)
	}
}

func TestRewriteManifestsFilterCarriesTheRest(t *testing.T) {
	f := newRewriteManifestsFixture(t, nil)
	tbl := f.interleave(f.tbl)
	_, ms := live(t, tbl)
	keep := map[string]bool{ms[0].FilePath(): true, ms[1].FilePath(): true}

	tbl, res := f.rewrite(tbl, WithRewriteManifestsFilter(func(m iceberg.ManifestFile) bool { return !keep[m.FilePath()] }))
	require.Equal(t, 10, res.SourceManifests)
	require.Equal(t, 1, res.NewManifests, "ten entries fit the default target")
	require.Equal(t, 2, res.KeptManifests)
	_, after := live(t, tbl)
	require.Len(t, after, 3)
	var carried int
	for _, m := range after {
		if keep[m.FilePath()] {
			carried++
		}
	}
	require.Equal(t, 2, carried, "the filtered-out manifests are referenced by path, untouched")
	require.Len(t, plannedFiles(t, tbl), 12)
}

func TestRewriteManifestsNothingToDo(t *testing.T) {
	f := newRewriteManifestsFixture(t, nil)
	txn := f.tbl.NewTransaction()
	res, err := txn.RewriteManifests(context.Background(), nil)
	require.NoError(t, err)
	require.Zero(t, res.SourceManifests, "an empty table has nothing to rewrite")

	tbl := f.commit(f.tbl, "a.parquet", 1)
	txn = tbl.NewTransaction()
	res, err = txn.RewriteManifests(context.Background(), nil, WithRewriteManifestsFilter(func(iceberg.ManifestFile) bool { return false }))
	require.NoError(t, err)
	require.Zero(t, res.SourceManifests)
	out, err := txn.Commit(context.Background())
	require.NoError(t, err)
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, out.CurrentSnapshot().SnapshotID, "no snapshot staged")
}

func TestRewriteManifestsRootModeRewritesChildrenCarriesInline(t *testing.T) {
	f := newRewriteManifestsFixture(t, rootProps(iceberg.Properties{RootManifestInlineMaxKey: "1"}))
	tbl := f.interleave(f.tbl)
	// The 12th commit promoted; one more leaves an entry inlined.
	tbl = f.commit(tbl, "inlined.parquet", 9)
	before, ms := live(t, tbl)
	children, inline := splitRoot(ms)
	require.NotNil(t, inline)
	require.Greater(t, len(children), 3)
	inlineFiles := inline.AddedDataFiles() + inline.ExistingDataFiles()

	tbl, res := f.rewrite(tbl)
	require.Equal(t, len(children), res.SourceManifests)
	require.Equal(t, 1, res.NewManifests)

	after, ms := live(t, tbl)
	require.Equal(t, before, after)
	children, inline = splitRoot(ms)
	require.Len(t, children, 1)
	require.NotNil(t, inline)
	require.Equal(t, inlineFiles, inline.AddedDataFiles()+inline.ExistingDataFiles(), "the inline section is carried, not folded")
	require.Len(t, plannedFiles(t, tbl), 13)

	// The table keeps working as a root-mode table after the rewrite.
	tbl = f.commit(tbl, "late.parquet", 10)
	require.Len(t, plannedFiles(t, tbl), 14)
}

// A concurrent append lands between planning the rewrite and committing it: the
// rebuild keeps the append's manifest next to ours, and nothing is planned twice.
func TestRewriteManifestsRebuildCarriesConcurrentAppend(t *testing.T) {
	for _, mode := range []string{"list", "root"} {
		t.Run(mode, func(t *testing.T) {
			props := iceberg.Properties{CommitNumRetriesKey: "3", CommitMinRetryWaitMsKey: "1", CommitMaxRetryWaitMsKey: "2", CommitTotalRetryTimeoutMsKey: "60000"}
			if mode == "root" {
				props = rootProps(props)
				props[RootManifestInlineMaxKey] = "1"
			}
			f := newRewriteManifestsFixture(t, props)
			tbl := f.interleave(f.tbl)

			txn := tbl.NewTransaction()
			_, err := txn.RewriteManifests(context.Background(), nil, WithRewriteManifestsTargetEntries(2))
			require.NoError(t, err)

			other := New(tbl.Identifier(), f.cat.metadata, "", tbl.FSFunc(), f.cat)
			f.commit(other, "concurrent.parquet", 10)
			f.cat.attempts.Store(0)
			f.cat.errs = []error{ErrCommitFailed}
			f.cat.loadMeta = f.cat.metadata

			out, err := txn.Commit(context.Background())
			require.NoError(t, err)
			entries, _ := live(t, out) // fails on any file live twice
			require.Len(t, entries, 13)
			require.Contains(t, plannedFiles(t, out), "concurrent.parquet")
		})
	}
}

// A concurrent overwrite replaced one of the sources to drop a file. Committing the
// rewrite would resurrect it, so the rebuild refuses and the table is unchanged.
func TestRewriteManifestsRefusesWhenASourceWasReplaced(t *testing.T) {
	f := newRewriteManifestsFixture(t, iceberg.Properties{
		CommitNumRetriesKey: "3", CommitMinRetryWaitMsKey: "1", CommitMaxRetryWaitMsKey: "2", CommitTotalRetryTimeoutMsKey: "60000",
	})
	tbl := f.commit(f.tbl, "a.parquet", 1)
	tbl = f.commit(tbl, "b.parquet", 1)
	// Two files in one manifest, so the delete rewrites the manifest rather than
	// dropping it.
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(context.Background(), []iceberg.DataFile{f.dataFile("c.parquet", 2), f.dataFile("d.parquet", 2)}, nil))
	tbl, err := txn.Commit(context.Background())
	require.NoError(t, err)

	rw := tbl.NewTransaction()
	_, err = rw.RewriteManifests(context.Background(), nil)
	require.NoError(t, err)

	other := New(tbl.Identifier(), f.cat.metadata, "", tbl.FSFunc(), f.cat)
	del := other.NewTransaction()
	require.NoError(t, del.ReplaceDataFiles(context.Background(), []string{f.warehouse + "/t/data/c.parquet"}, nil, nil))
	other, err = del.Commit(context.Background())
	require.NoError(t, err)
	f.cat.attempts.Store(0)
	f.cat.errs = []error{ErrCommitFailed}
	f.cat.loadMeta = f.cat.metadata
	headBefore := f.cat.metadata.CurrentSnapshot().SnapshotID

	_, err = rw.Commit(context.Background())
	require.ErrorIs(t, err, ErrCommitDiverged)
	require.Equal(t, headBefore, f.cat.metadata.CurrentSnapshot().SnapshotID, "nothing committed")
	require.NotContains(t, plannedFiles(t, other), "c.parquet")
}
