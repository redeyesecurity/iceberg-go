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
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// placingCatalog places each new metadata file the way a catalog built on
// UpdateAndStageTable does, through the location provider of the committed
// metadata, so the location field picks the directory. Each commit trims the
// log to its last keep entries and appends the file it replaced, the way
// write.metadata.previous-versions-max=keep+1 would, and deleteOldMetadata
// sees what was trimmed. onFail, when set, runs on each of the first
// failFirst attempts before they are refused as a lost race.
type placingCatalog struct {
	keep      int
	metadata  Metadata
	location  string
	version   int
	commits   int
	attempts  int
	failFirst int
	onFail    func(*placingCatalog)
}

func (c *placingCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	return New(ident, c.metadata, c.location,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *placingCatalog) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	c.attempts++
	if c.attempts <= c.failFirst {
		if c.onFail != nil {
			c.onFail(c)
		}

		return nil, "", fmt.Errorf("pointer race: %w", ErrCommitFailed)
	}

	bldr, err := MetadataBuilderFromBase(c.metadata, "")
	if err != nil {
		return nil, "", err
	}
	bldr.TrimMetadataLogs(c.keep)
	bldr.AppendMetadataLog(MetadataLogEntry{MetadataFile: c.location, TimestampMs: time.Now().UnixMilli()})
	for _, u := range updates {
		if err := u.Apply(bldr); err != nil {
			return nil, "", err
		}
	}
	meta, err := bldr.Build()
	if err != nil {
		return nil, "", err
	}
	provider, err := LoadLocationProvider(meta.Location(), meta.Properties())
	if err != nil {
		return nil, "", err
	}
	c.version++
	loc, err := provider.NewTableMetadataFileLocation(c.version + 1)
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(filepath.Dir(loc), 0o755); err != nil {
		return nil, "", err
	}
	if err := os.WriteFile(loc, []byte("{}"), 0o644); err != nil {
		return nil, "", err
	}
	c.metadata, c.location = meta, loc
	c.commits++

	return meta, loc, nil
}

// anchorFixture is table T at <wh>/t, loaded from a head file in
// <wh>/t/metadata, next to table V at <wh>/hot, whose head is victimHead.
type anchorFixture struct {
	wh, loc, victimHead string
	cat                 *placingCatalog
}

func newAnchorFixture(t *testing.T, props iceberg.Properties) anchorFixture {
	t.Helper()
	wh := filepath.ToSlash(t.TempDir())
	loc := wh + "/t"
	all := iceberg.Properties{
		PropertyFormatVersion:               "2",
		MetadataDeleteAfterCommitEnabledKey: "true",
	}
	for k, v := range props {
		all[k] = v
	}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, all)
	require.NoError(t, err)

	head := loc + "/metadata/00001-t.metadata.json"
	victimHead := wh + "/hot/metadata/00001-victim.metadata.json"
	touch(t, head)
	touch(t, victimHead)

	return anchorFixture{wh: wh, loc: loc, victimHead: victimHead, cat: &placingCatalog{metadata: meta, location: head}}
}

// plantInPlace rewrites the catalog's current metadata the way a writer who
// can overwrite metadata.json in place would: the location field moved to
// newLoc and the log naming entries. The catalog pointer does not move.
func (f anchorFixture) plantInPlace(t *testing.T, newLoc string, entries ...string) {
	t.Helper()
	f.cat.metadata = plantedMetadata(t, f.cat.metadata, newLoc, entries...)
}

func plantedMetadata(t *testing.T, base Metadata, newLoc string, entries ...string) Metadata {
	t.Helper()
	bldr, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, bldr.SetLoc(newLoc))
	for _, e := range entries {
		bldr.AppendMetadataLog(MetadataLogEntry{MetadataFile: e, TimestampMs: time.Now().UnixMilli()})
	}
	meta, err := bldr.Build()
	require.NoError(t, err)

	return meta
}

// commit loads the table from the catalog, as an ordinary writer does, and
// commits once through doCommit with fsys as its FileIO.
func (f anchorFixture) commit(t *testing.T, fsys iceio.IO, updates ...Update) (*Table, error) {
	t.Helper()
	tbl := New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location,
		func(context.Context) (iceio.IO, error) { return fsys, nil }, f.cat)

	return tbl.doCommit(t.Context(), updates, nil)
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}

	return out
}

// The caver-go#9536 two-commit bypass of the redeye.17 delete anchor. A plant
// in T's head moves location to V's directory and names V's head in the log,
// on a table that keeps two log entries. Commit 1 used to place T's next head
// in <wh>/hot/metadata, where the location provider read it from, and the
// pointer followed; the planted entry was carried, not trimmed. Commit 2 was
// then loaded from <wh>/hot/metadata, so its delete anchor was V's directory,
// and it trimmed and deleted V's head. Commit 1 is now refused before
// the catalog sees it, nothing is written into V's directory, the pointer
// stays on T, and every later commit on the planted base is refused the same
// way.
func TestALocationMovingPlantCannotWriteIntoAnotherTable9536(t *testing.T) {
	f := newAnchorFixture(t, nil)
	f.cat.keep = 1
	head := f.cat.location
	f.plantInPlace(t, f.wh+"/hot", f.victimHead)

	for i := 1; i <= 2; i++ {
		fsys := &removeRecordingIO{}
		_, err := f.commit(t, fsys)
		require.ErrorIs(t, err, ErrMetadataWriteRefused, "commit %d on the planted base", i)
		assert.Contains(t, err.Error(), f.loc+"/metadata")
		assert.Contains(t, err.Error(), f.wh+"/hot/metadata")

		assert.Zero(t, f.cat.attempts, "commit %d never reaches the catalog", i)
		assert.Equal(t, head, f.cat.location, "the pointer stays on T's own head")
		assert.Equal(t, []string{"00001-victim.metadata.json"}, listDir(t, f.wh+"/hot/metadata"),
			"nothing is written into the other table's directory")
		assert.FileExists(t, f.victimHead)
		assert.Empty(t, fsys.removed, "a refused commit deletes nothing")
	}
}

// The same plant aimed at the warehouse root, the widest directory a moved
// location can name.
func TestAPlantMovingLocationToTheWarehouseRootIsRefused9536(t *testing.T) {
	f := newAnchorFixture(t, nil)
	f.plantInPlace(t, f.wh)

	_, err := f.commit(t, &removeRecordingIO{})
	require.ErrorIs(t, err, ErrMetadataWriteRefused)
	assert.Zero(t, f.cat.attempts)
	assert.NoDirExists(t, f.wh+"/metadata")
}

// A retry commits on the base it refreshed to, so a plant that lands while
// the writer is backing off is refused on the retry, not carried through.
func TestARetryRefreshedOntoAPlantIsRefused9536(t *testing.T) {
	f := newAnchorFixture(t, iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	victimHead := f.victimHead
	f.cat.failFirst = 1
	f.cat.onFail = func(c *placingCatalog) {
		c.metadata = plantedMetadata(t, c.metadata, f.wh+"/hot", victimHead)
	}

	_, err := f.commit(t, &removeRecordingIO{})
	require.ErrorIs(t, err, ErrMetadataWriteRefused)
	assert.Equal(t, 1, f.cat.attempts, "the refused retry never reaches the catalog")
	assert.Zero(t, f.cat.commits)
	assert.Equal(t, []string{"00001-victim.metadata.json"}, listDir(t, f.wh+"/hot/metadata"))
	assert.FileExists(t, victimHead)
}

// An honest table keeps committing: two commits land in <wh>/t/metadata, and
// the second one's cleanup deletes the head the first one trimmed from the
// log.
func TestAnHonestTableCommitsAndCleansUp9536(t *testing.T) {
	f := newAnchorFixture(t, nil)
	first := f.cat.location

	fsys := &removeRecordingIO{}
	tbl, err := f.commit(t, fsys)
	require.NoError(t, err)
	second := tbl.MetadataLocation()
	assert.Equal(t, f.loc+"/metadata", filepath.ToSlash(filepath.Dir(second)))

	tbl, err = f.commit(t, fsys)
	require.NoError(t, err)
	assert.Equal(t, f.loc+"/metadata", filepath.ToSlash(filepath.Dir(tbl.MetadataLocation())))

	assert.Equal(t, []string{first}, fsys.removed)
	assert.NoFileExists(t, first)
	assert.FileExists(t, second)
	assert.Equal(t, 2, f.cat.commits)
	assert.FileExists(t, f.victimHead)
}

// A commit that itself sets a new location is an update the caller sent, not
// one read from storage: its base is consistent, so it lands and its file is
// placed in the new location.
func TestACommitThatSetsLocationStillLands9536(t *testing.T) {
	f := newAnchorFixture(t, nil)

	tbl, err := f.commit(t, &removeRecordingIO{}, NewSetLocationUpdate(f.wh+"/t-moved"))
	require.NoError(t, err)
	assert.Equal(t, f.wh+"/t-moved/metadata", filepath.ToSlash(filepath.Dir(tbl.MetadataLocation())))
}

// write.metadata.path moves where a table keeps its metadata. A table loaded
// from that directory commits; a plant that sets the property to another
// table's directory is refused like a moved location.
func TestWriteMetadataPathIsHeldToTheLoadedDirectory9536(t *testing.T) {
	t.Run("loaded from the property's directory", func(t *testing.T) {
		wh := filepath.ToSlash(t.TempDir())
		f := newAnchorFixture(t, iceberg.Properties{WriteMetadataPathKey: wh + "/meta-t"})
		// The fixture's warehouse differs from wh; re-home the head so the
		// pointer names the property's directory.
		f.cat.location = wh + "/meta-t/00001-t.metadata.json"
		touch(t, f.cat.location)

		tbl, err := f.commit(t, &removeRecordingIO{})
		require.NoError(t, err)
		assert.Equal(t, wh+"/meta-t", filepath.ToSlash(filepath.Dir(tbl.MetadataLocation())))
	})
	t.Run("a planted property", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		bldr, err := MetadataBuilderFromBase(f.cat.metadata, "")
		require.NoError(t, err)
		require.NoError(t, bldr.SetProperties(iceberg.Properties{WriteMetadataPathKey: f.wh + "/hot/metadata"}))
		meta, err := bldr.Build()
		require.NoError(t, err)
		f.cat.metadata = meta

		_, err = f.commit(t, &removeRecordingIO{})
		require.ErrorIs(t, err, ErrMetadataWriteRefused)
		assert.Zero(t, f.cat.attempts)
	})
}

func TestCheckMetadataWriteAnchor9536(t *testing.T) {
	newMeta := func(t *testing.T, loc string, props iceberg.Properties) Metadata {
		t.Helper()
		all := iceberg.Properties{PropertyFormatVersion: "2"}
		for k, v := range props {
			all[k] = v
		}
		meta, err := NewMetadata(iceberg.NewSchema(0), iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, all)
		require.NoError(t, err)

		return meta
	}
	cases := []struct {
		name, base, loc, loaded string
		props                   iceberg.Properties
		ok                      bool
	}{
		{"local", "", "/w/t", "/w/t/metadata/00001-a.metadata.json", nil, true},
		{"location with a trailing slash", "", "/w/t/", "/w/t/metadata/00001-a.metadata.json", nil, true},
		{"s3", "", "s3://lake/w/t", "s3://lake/w/t/metadata/00001-a.metadata.json", nil, true},
		{"file URI location, bare loaded path", "", "file:///w/t", "/w/t/metadata/00001-a.metadata.json", nil, true},
		{"relative loaded path against the base", "/w", "/w/t", "t/metadata/00001-a.metadata.json", nil, true},
		{"write.metadata.path", "", "/w/t", "/w/meta/00001-a.metadata.json",
			iceberg.Properties{WriteMetadataPathKey: "/w/meta"}, true},
		{"no loaded path", "", "/w/t", "", nil, true},
		{"bare placeholder name", "", "/w/t", "metadata.json", nil, true},

		{"location moved to another table", "", "/w/hot", "/w/t/metadata/00001-a.metadata.json", nil, false},
		{"location moved to the warehouse root", "", "/w", "/w/t/metadata/00001-a.metadata.json", nil, false},
		{"sibling sharing a prefix", "", "/w/t2", "/w/t/metadata/00001-a.metadata.json", nil, false},
		{"loaded from a subdirectory", "", "/w/t", "/w/t/metadata/x/00001-a.metadata.json", nil, false},
		{"another bucket", "", "s3://other/w/t", "s3://lake/w/t/metadata/00001-a.metadata.json", nil, false},
		{"another scheme", "", "gs://lake/w/t", "s3://lake/w/t/metadata/00001-a.metadata.json", nil, false},
		{"relative loaded path with no base", "", "/w/t", "t/metadata/00001-a.metadata.json", nil, false},
		{"write.metadata.path planted elsewhere", "", "/w/t", "/w/t/metadata/00001-a.metadata.json",
			iceberg.Properties{WriteMetadataPathKey: "/w/hot/metadata"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckMetadataWriteAnchor(tc.base, newMeta(t, tc.loc, tc.props), tc.loaded)
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, ErrMetadataWriteRefused)
			}
		})
	}
}
