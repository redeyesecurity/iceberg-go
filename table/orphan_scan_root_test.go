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
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// orphanPlantTables builds a warehouse with two tables, db.t and db.hot, each
// with its head metadata file and a data file on disk, and returns db.t loaded
// from its own pointer but parsed with location set to loc (caver-go#9630).
func orphanPlantTables(t *testing.T, loc func(warehouse string) string) (*Table, string) {
	t.Helper()
	dir := filepath.ToSlash(t.TempDir())
	warehouse := "file://" + dir + "/wh"
	for _, name := range []string{"t", "hot"} {
		require.NoError(t, os.MkdirAll(dir+"/wh/"+name+"/metadata", 0o755))
		require.NoError(t, os.MkdirAll(dir+"/wh/"+name+"/data", 0o755))
		require.NoError(t, os.WriteFile(dir+"/wh/"+name+"/metadata/v1.metadata.json", []byte("{}"), 0o644))
		require.NoError(t, os.WriteFile(dir+"/wh/"+name+"/data/f.parquet", []byte("x"), 0o644))
	}
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, loc(warehouse),
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	tbl := New(Identifier{"db", "t"}, meta, warehouse+"/t/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil)

	return tbl, warehouse
}

// A location planted onto another table's directory must not turn that
// table's files into orphans: the default root is anchored on the loaded
// metadata file and the disagreement is refused before anything is listed.
func TestOrphanScanRefusesPlantedLocation(t *testing.T) {
	for name, loc := range map[string]func(string) string{
		"sibling table": func(wh string) string { return wh + "/hot" },
		"warehouse":     func(wh string) string { return wh },
		"inside table":  func(wh string) string { return wh + "/t/data" },
	} {
		t.Run(name, func(t *testing.T) {
			tbl, warehouse := orphanPlantTables(t, loc)
			res, err := tbl.DeleteOrphanFiles(context.Background(), WithDryRun(true), WithFilesOlderThan(-time.Hour))
			require.ErrorIs(t, err, ErrOrphanScanRefused)
			require.Empty(t, res.OrphanFileLocations)

			// Not a dry run either: nothing may be removed.
			_, err = tbl.DeleteOrphanFiles(context.Background(), WithFilesOlderThan(-time.Hour))
			require.ErrorIs(t, err, ErrOrphanScanRefused)
			dir := filepath.FromSlash(warehouse[len("file://"):])
			for _, f := range []string{"hot/metadata/v1.metadata.json", "hot/data/f.parquet", "t/data/f.parquet"} {
				_, statErr := os.Stat(filepath.Join(dir, f))
				require.NoError(t, statErr, "%s must survive a refused sweep", f)
			}
		})
	}
}

// An honest table still sweeps its own root, and only its own root.
func TestOrphanScanAnchoredRootSweepsOwnTable(t *testing.T) {
	tbl, _ := orphanPlantTables(t, func(wh string) string { return wh + "/t" })
	res, err := tbl.DeleteOrphanFiles(context.Background(), WithDryRun(true), WithFilesOlderThan(-time.Hour),
		WithEqualSchemes(map[string]string{"file": ""}))
	require.NoError(t, err)
	require.Len(t, res.OrphanFileLocations, 1, "%v", res.OrphanFileLocations)
	require.Contains(t, res.OrphanFileLocations[0], "/wh/t/data/f.parquet")
}

// A caller that names the root keeps the old behaviour: WithLocation is the
// caller's choice, not a string read from storage.
func TestOrphanScanExplicitLocationIsHonoured(t *testing.T) {
	tbl, warehouse := orphanPlantTables(t, func(wh string) string { return wh + "/hot" })
	res, err := tbl.DeleteOrphanFiles(context.Background(), WithLocation(warehouse+"/t"), WithDryRun(true),
		WithFilesOlderThan(-time.Hour), WithEqualSchemes(map[string]string{"file": ""}))
	require.NoError(t, err)
	for _, p := range res.OrphanFileLocations {
		require.NotContains(t, p, "/wh/hot/", "%v", res.OrphanFileLocations)
	}
}

func TestOrphanScanRoot(t *testing.T) {
	cases := []struct {
		name, base, location, loaded, want string
	}{
		{name: "agrees", location: "s3://b/w/t", loaded: "s3://b/w/t/metadata/v3.metadata.json", want: "s3://b/w/t"},
		{name: "trailing slash", location: "s3://b/w/t/", loaded: "s3://b/w/t/metadata/v3.metadata.json", want: "s3://b/w/t/"},
		{name: "file uri and bare path", location: "file:///w/t", loaded: "/w/t/metadata/v1.metadata.json", want: "file:///w/t"},
		{name: "relative pointer under a base", base: "file:///w", location: "file:///w/t", loaded: "t/metadata/v1.metadata.json", want: "file:///w/t"},
		{name: "sibling", location: "s3://b/w/hot", loaded: "s3://b/w/t/metadata/v3.metadata.json"},
		{name: "other bucket", location: "s3://evil/w/t", loaded: "s3://b/w/t/metadata/v3.metadata.json"},
		{name: "parent", location: "s3://b/w", loaded: "s3://b/w/t/metadata/v3.metadata.json"},
		{name: "dot dot", location: "s3://b/w/t/../hot", loaded: "s3://b/w/t/metadata/v3.metadata.json"},
		{name: "custom metadata path", location: "s3://b/w/t", loaded: "s3://b/meta/t/v3.metadata.json"},
		{name: "empty location", location: "", loaded: "s3://b/w/t/metadata/v3.metadata.json"},
		{name: "no loaded path", location: "s3://b/w/t", loaded: ""},
		{name: "bare file name", location: "s3://b/w/t", loaded: "v1.metadata.json"},
		{name: "relative pointer, no base", location: "s3://b/w/t", loaded: "t/metadata/v1.metadata.json"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := orphanScanRoot(c.base, c.location, c.loaded)
			if c.want == "" {
				require.ErrorIs(t, err, ErrOrphanScanRefused)

				return
			}
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}
