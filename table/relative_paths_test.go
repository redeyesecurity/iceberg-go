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

// Relative paths (io.RelativePathsKey, caver-go#4441): manifests and manifest lists
// store warehouse-relative paths, readers see absolute ones, and a copied tree reads
// from a new root without any metadata rewrite.

func newRelativePathsTable(t *testing.T, warehouse string, props iceberg.Properties) (*Table, *sequentialCatalog) {
	t.Helper()
	location := warehouse + "/t"
	if props == nil {
		props = iceberg.Properties{}
	}
	props[PropertyFormatVersion] = "2"
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, location, props)
	require.NoError(t, err)
	cat := &sequentialCatalog{metadata: meta}
	fsys := iceio.WrapRelative(iceio.LocalFS{}, warehouse)
	tbl := New(Identifier{"db", "rel"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return fsys, nil }, cat)

	return tbl, cat
}

func commitOneFile(t *testing.T, tbl *Table, path string) *Table {
	t.Helper()
	ctx := context.Background()
	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	require.NoError(t, fsys.(iceio.WriteFileIO).WriteFile(path, []byte("not really parquet")))
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(ctx, []iceberg.DataFile{newTestDataFile(t, *iceberg.UnpartitionedSpec, path, nil)}, nil))
	out, err := txn.Commit(ctx)
	require.NoError(t, err)

	return out
}

// storedManifests decodes a manifest list WITHOUT a base: manifests whose paths are
// exactly as stored on disk.
func storedManifests(t *testing.T, listPath string) []iceberg.ManifestFile {
	t.Helper()
	f, err := os.Open(strings.TrimPrefix(listPath, "file://"))
	require.NoError(t, err)
	defer f.Close()
	files, err := iceberg.ReadManifestList(f)
	require.NoError(t, err)

	return files
}

// storedDataPaths decodes one base-less manifest: data-file paths as stored on disk.
func storedDataPaths(t *testing.T, warehouse string, m iceberg.ManifestFile) []string {
	t.Helper()
	f, err := os.Open(strings.TrimPrefix(iceio.JoinBase(warehouse, m.FilePath()), "file://"))
	require.NoError(t, err)
	defer f.Close()
	entries, err := iceberg.ReadManifest(m, f, false)
	require.NoError(t, err)
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.DataFile().FilePath())
	}

	return out
}

func TestRelativePathsWrittenRelativeReadAbsolute(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	tbl, _ := newRelativePathsTable(t, warehouse, iceberg.Properties{RelativePathsKey: iceio.RelativePathsWarehouse})
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))

	dataPath := warehouse + "/t/data/f1.parquet"
	tbl = commitOneFile(t, tbl, dataPath)
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)

	// The snapshot's manifest-list reference is warehouse-relative.
	require.Equal(t, "t/metadata/", snap.ManifestList[:len("t/metadata/")], "manifest list path must be relative to the warehouse: %q", snap.ManifestList)

	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := snap.Manifests(fsys)
	require.NoError(t, err)
	require.Len(t, manifests, 1)
	// In memory the manifest path is absolute; on disk the list stores it relative.
	require.True(t, strings.HasPrefix(manifests[0].FilePath(), warehouse+"/t/metadata/"), "manifest path in memory: %q", manifests[0].FilePath())
	stored := storedManifests(t, iceio.JoinBase(warehouse, snap.ManifestList))
	require.Len(t, stored, 1)
	require.True(t, strings.HasPrefix(stored[0].FilePath(), "t/metadata/"), "manifest list on disk must store a warehouse-relative path: %q", stored[0].FilePath())

	entries, err := manifests[0].FetchEntries(fsys, false)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, dataPath, entries[0].DataFile().FilePath(), "data path in memory must be absolute")
	require.Equal(t, []string{"t/data/f1.parquet"}, storedDataPaths(t, warehouse, stored[0]), "manifest on disk must store a warehouse-relative data path")

	// Planning sees absolute paths too.
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, dataPath, tasks[0].File.FilePath())

	// A second commit references the first manifest through the list again (existing
	// manifests are carried by path), still relative on disk, still absolute in memory.
	tbl = commitOneFile(t, tbl, warehouse+"/t/data/f2.parquet")
	manifests, err = tbl.CurrentSnapshot().Manifests(fsys)
	require.NoError(t, err)
	require.Len(t, manifests, 2)
	for _, m := range manifests {
		require.True(t, strings.HasPrefix(m.FilePath(), warehouse+"/t/metadata/"), m.FilePath())
	}
	tasks, err = tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	// The orphan sweep's referenced set resolves the relative paths, so nothing the
	// table owns is an orphan and a real stray still is.
	require.NoError(t, os.WriteFile(dir+"/wh/t/data/stray.parquet", []byte("x"), 0o644))
	res, err := tbl.DeleteOrphanFiles(ctx, WithDryRun(true), WithFilesOlderThan(0), WithEqualSchemes(map[string]string{"file": ""}))
	require.NoError(t, err)
	require.Len(t, res.OrphanFileLocations, 1, "only the stray may be an orphan: %v", res.OrphanFileLocations)
	require.Contains(t, res.OrphanFileLocations[0], "stray.parquet")
}

func TestRelativePathsOffWritesAbsolute(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	// FileIO has a base but the table did not ask: absolute, as before.
	tbl, _ := newRelativePathsTable(t, warehouse, nil)
	tbl = commitOneFile(t, tbl, warehouse+"/t/data/f1.parquet")
	snap := tbl.CurrentSnapshot()
	require.True(t, strings.HasPrefix(snap.ManifestList, warehouse), snap.ManifestList)
	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := snap.Manifests(fsys)
	require.NoError(t, err)
	stored := storedManifests(t, snap.ManifestList)
	require.True(t, strings.HasPrefix(stored[0].FilePath(), warehouse), stored[0].FilePath())
	require.True(t, strings.HasPrefix(storedDataPaths(t, warehouse, stored[0])[0], warehouse))
	require.Len(t, manifests, 1)
}

func TestRelativePathsTreeReadsFromANewRoot(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	oldRoot := "file://" + dir + "/wh"
	require.NoError(t, os.MkdirAll(dir+"/wh/t/data", 0o755))
	require.NoError(t, os.MkdirAll(dir+"/wh/t/metadata", 0o755))
	tbl, _ := newRelativePathsTable(t, oldRoot, iceberg.Properties{RelativePathsKey: iceio.RelativePathsWarehouse})
	tbl = commitOneFile(t, tbl, oldRoot+"/t/data/f1.parquet")
	tbl = commitOneFile(t, tbl, oldRoot+"/t/data/f2.parquet")

	// Move the whole tree. No metadata file is touched.
	newDir := filepath.ToSlash(t.TempDir())
	require.NoError(t, os.Rename(dir+"/wh", newDir+"/moved"))
	newRoot := "file://" + newDir + "/moved"

	// Same metadata object (its `location` still names the old root; only the
	// FileIO's base changed), read from the new root.
	moved := New(Identifier{"db", "rel"}, tbl.Metadata(), newRoot+"/t/metadata/v3.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.WrapRelative(iceio.LocalFS{}, newRoot), nil }, nil)
	fsys, err := moved.FS(ctx)
	require.NoError(t, err)
	manifests, err := moved.CurrentSnapshot().Manifests(fsys)
	require.NoError(t, err)
	require.Len(t, manifests, 2)
	tasks, err := moved.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	for _, task := range tasks {
		p := task.File.FilePath()
		require.True(t, strings.HasPrefix(p, newRoot+"/t/data/"), "data path must resolve under the new root: %q", p)
		_, err := os.Stat(strings.TrimPrefix(p, "file://"))
		require.NoError(t, err, "the moved data file must exist where the path says")
	}
	// And the old root is gone, so nothing resolved there by accident.
	_, err = os.Stat(dir + "/wh")
	require.True(t, os.IsNotExist(err))
}
