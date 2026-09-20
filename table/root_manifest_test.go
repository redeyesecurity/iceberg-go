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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// Root manifest (caver-go#4443): one file per commit, inlined entries carried and
// promoted, deletes honoured, the sweep and the OCC rebuild both root-aware.

func metadataFiles(t *testing.T, warehouse string) (roots, manifests int) {
	t.Helper()
	entries, err := os.ReadDir(strings.TrimPrefix(warehouse, "file://") + "/t/metadata")
	require.NoError(t, err)
	for _, e := range entries {
		switch {
		case strings.HasPrefix(e.Name(), "snap-"):
			roots++
		case strings.Contains(e.Name(), "-m") && strings.HasSuffix(e.Name(), ".avro"):
			manifests++
		}
	}

	return roots, manifests
}

func rootProps(extra iceberg.Properties) iceberg.Properties {
	p := iceberg.Properties{RootManifestKey: RootManifestInline}
	for k, v := range extra {
		p[k] = v
	}

	return p
}

func TestRootManifestOneFilePerCommit(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	tbl, _ := newRelativePathsTable(t, warehouse, rootProps(nil))
	for i := 0; i < 3; i++ {
		tbl = commitOneFile(t, tbl, warehouse+"/t/data/f"+string(rune('a'+i))+".parquet")
	}
	roots, manifests := metadataFiles(t, warehouse)
	require.Equal(t, 3, roots, "one root per commit")
	require.Equal(t, 0, manifests, "no per-commit manifest: added files are inlined")

	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	ms, err := tbl.CurrentSnapshot().Manifests(fsys)
	require.NoError(t, err)
	require.Len(t, ms, 1, "the snapshot's manifests are just the inline record")
	require.True(t, iceberg.IsInlineRootManifest(ms[0]))
	require.Equal(t, iceio.JoinBase(warehouse, tbl.CurrentSnapshot().ManifestList), ms[0].FilePath(), "the inline record's path is the root itself")
	require.Equal(t, int32(1), ms[0].AddedDataFiles())
	require.Equal(t, int32(2), ms[0].ExistingDataFiles(), "earlier commits' files are carried as EXISTING")

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	// Entries carry their original snapshot ids.
	entries, err := ms[0].FetchEntries(fsys, false)
	require.NoError(t, err)
	ids := map[int64]int{}
	for _, e := range entries {
		ids[e.SnapshotID()]++
	}
	require.Len(t, ids, 3, "three commits, three distinct snapshot ids on the inlined entries: %v", ids)
}

func TestRootManifestPromotesPastTheInlineBound(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	tbl, _ := newRelativePathsTable(t, warehouse, rootProps(iceberg.Properties{RootManifestInlineMaxKey: "2"}))
	for i := 0; i < 5; i++ {
		tbl = commitOneFile(t, tbl, warehouse+"/t/data/f"+string(rune('a'+i))+".parquet")
	}
	// Commits 1,2 inline (1, 2 entries); commit 3 would inline 3 > 2: promote to a
	// child, root empty; commit 4 inlines 1; commit 5 inlines 2. So: one child, and a
	// root inline of 2.
	roots, manifests := metadataFiles(t, warehouse)
	require.Equal(t, 5, roots)
	require.Equal(t, 1, manifests, "exactly one promotion child")
	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	ms, err := tbl.CurrentSnapshot().Manifests(fsys)
	require.NoError(t, err)
	require.Len(t, ms, 2)
	children, inline := splitRoot(ms)
	require.Len(t, children, 1)
	require.NotNil(t, inline)
	require.Equal(t, int32(3), children[0].ExistingDataFiles()+children[0].AddedDataFiles(), "the child holds the three promoted files (two carried, one added by the promoting commit)")
	require.Equal(t, int32(2), inline.AddedDataFiles()+inline.ExistingDataFiles())
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 5, "every file planned exactly once across child and root")
}

func TestRootManifestHonoursDeletes(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	tbl, _ := newRelativePathsTable(t, warehouse, rootProps(nil))
	tbl = commitOneFile(t, tbl, warehouse+"/t/data/fa.parquet")
	tbl = commitOneFile(t, tbl, warehouse+"/t/data/fb.parquet")
	// Remove an inlined file the standard way.
	txn := tbl.NewTransaction()
	require.NoError(t, txn.ReplaceDataFiles(ctx, []string{warehouse + "/t/data/fa.parquet"}, nil, nil))
	tbl, err := txn.Commit(ctx)
	require.NoError(t, err)
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, warehouse+"/t/data/fb.parquet", tasks[0].File.FilePath())
	// And a later append still carries the survivor.
	tbl = commitOneFile(t, tbl, warehouse+"/t/data/fc.parquet")
	tasks, err = tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
}

func TestRootManifestSweepAndRelativePaths(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	tbl, _ := newRelativePathsTable(t, warehouse, rootProps(iceberg.Properties{RelativePathsKey: iceio.RelativePathsWarehouse, RootManifestInlineMaxKey: "1"}))
	for i := 0; i < 3; i++ {
		tbl = commitOneFile(t, tbl, warehouse+"/t/data/f"+string(rune('a'+i))+".parquet")
	}
	// Relative: the snapshot's root reference and, inside the header, the child path.
	require.True(t, strings.HasPrefix(tbl.CurrentSnapshot().ManifestList, "t/metadata/"), tbl.CurrentSnapshot().ManifestList)
	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	ms, err := tbl.CurrentSnapshot().Manifests(fsys)
	require.NoError(t, err)
	for _, m := range ms {
		require.True(t, strings.HasPrefix(m.FilePath(), warehouse+"/t/metadata/"), "resolved absolute in memory: %q", m.FilePath())
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	for _, task := range tasks {
		require.True(t, strings.HasPrefix(task.File.FilePath(), warehouse+"/t/data/"), task.File.FilePath())
	}
	// Sweep: roots and children are referenced; only the stray is an orphan.
	require.NoError(t, os.WriteFile(dir+"/wh/t/metadata/stray.avro", []byte("x"), 0o644))
	res, err := tbl.DeleteOrphanFiles(ctx, WithDryRun(true), WithFilesOlderThan(0), WithEqualSchemes(map[string]string{"file": ""}))
	require.NoError(t, err)
	require.Len(t, res.OrphanFileLocations, 1, "%v", res.OrphanFileLocations)
	require.Contains(t, res.OrphanFileLocations[0], "stray.avro")
}

// TestRootManifestRebuildsOnConflict: the catalog rejects the first commit attempt
// (ErrCommitFailed), the rebuild runs against the fresh parent, and the result is a
// root that holds both the concurrent commit's files and ours.
func TestRootManifestRebuildsOnConflict(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	tbl, cat := newRelativePathsTable(t, warehouse, rootProps(iceberg.Properties{
		CommitNumRetriesKey: "3", CommitMinRetryWaitMsKey: "1", CommitMaxRetryWaitMsKey: "2", CommitTotalRetryTimeoutMsKey: "60000",
	}))
	tbl = commitOneFile(t, tbl, warehouse+"/t/data/fa.parquet")

	// A concurrent writer lands fb through the catalog while we prepare fc.
	other := New(Identifier{"db", "rel"}, cat.metadata, "", tbl.FSFunc(), cat)
	other = commitOneFile(t, other, warehouse+"/t/data/fb.parquet")
	fresh := cat.metadata

	// Our commit is based on the stale table (fa only); the catalog fails it once so
	// doCommit rebuilds against fresh, then accepts.
	cat.attempts.Store(0) // the failure schedule is indexed by attempt; earlier commits used some
	cat.errs = []error{ErrCommitFailed}
	cat.loadMeta = fresh
	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	require.NoError(t, fsys.(iceio.WriteFileIO).WriteFile(warehouse+"/t/data/fc.parquet", []byte("x")))
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(ctx, []iceberg.DataFile{newTestDataFile(t, *iceberg.UnpartitionedSpec, warehouse+"/t/data/fc.parquet", nil)}, nil))
	out, err := txn.Commit(ctx)
	require.NoError(t, err)
	tasks, err := out.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	paths := make([]string, 0, len(tasks))
	for _, task := range tasks {
		paths = append(paths, filepath.Base(task.File.FilePath()))
	}
	require.ElementsMatch(t, []string{"fa.parquet", "fb.parquet", "fc.parquet"}, paths, "the rebuilt root must hold the concurrent commit's file and ours")
	ms, err := out.CurrentSnapshot().Manifests(fsys)
	require.NoError(t, err)
	require.Len(t, ms, 1, "still a single inline record after the rebuild")
}
