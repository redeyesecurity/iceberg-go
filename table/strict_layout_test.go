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
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// strictObservingCatalog is the placing catalog plus the refusal observer, so a
// test can see what a strict refusal tells the catalog.
type strictObservingCatalog struct {
	*placingCatalog
	seen []error
}

func (c *strictObservingCatalog) ObserveCommitRefusal(_ context.Context, _ Identifier, err error) {
	c.seen = append(c.seen, err)
}

// commitAs commits f's table through cat with strict on or off.
func (f anchorFixture) commitAs(t *testing.T, strict bool, cat CatalogIO, updates ...Update) (*Table, error) {
	t.Helper()
	tbl := New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location,
		func(context.Context) (iceio.IO, error) { return &removeRecordingIO{}, nil }, cat)
	if strict {
		tbl = tbl.WithStrictLayout()
	}

	return tbl.doCommit(t.Context(), updates, nil)
}

// withProps is f's current metadata with props set, the way a plant that
// rewrites metadata.json in place would set them.
func (f anchorFixture) withProps(t *testing.T, props iceberg.Properties) {
	t.Helper()
	bldr, err := MetadataBuilderFromBase(f.cat.metadata, "")
	require.NoError(t, err)
	require.NoError(t, bldr.SetProperties(props))
	meta, err := bldr.Build()
	require.NoError(t, err)
	f.cat.metadata = meta
}

func TestCheckStrictLayout9633(t *testing.T) {
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
		{"plain local", "", "/w/t", "/w/t/metadata/00001-a.metadata.json", nil, true},
		{"plain s3", "", "s3://lake/w/t", "s3://lake/w/t/metadata/00001-a.metadata.json", nil, true},
		{"trailing slash on the location", "", "/w/t/", "/w/t/metadata/00001-a.metadata.json", nil, true},
		{"relative loaded path against the base", "/w", "/w/t", "t/metadata/00001-a.metadata.json", nil, true},
		{"no loaded path", "", "/w/t", "", nil, true},
		{"bare placeholder name", "", "/w/t", "metadata.json", nil, true},
		{"properties spelling the default placement", "", "/w/t", "/w/t/metadata/00001-a.metadata.json",
			iceberg.Properties{WriteDataPathKey: "/w/t/data", WriteMetadataPathKey: "/w/t/metadata/"}, true},
		{"object storage explicitly off", "", "/w/t", "/w/t/metadata/00001-a.metadata.json",
			iceberg.Properties{ObjectStoreEnabledKey: "false"}, true},

		{"write.data.path elsewhere", "", "/w/t", "/w/t/metadata/00001-a.metadata.json",
			iceberg.Properties{WriteDataPathKey: "/w/hot/data"}, false},
		{"write.data.path under another table with no loaded path", "", "/w/t", "",
			iceberg.Properties{WriteDataPathKey: "/w/hot/data"}, false},
		{"write.metadata.path elsewhere, loaded from there", "", "/w/t", "/w/meta/00001-a.metadata.json",
			iceberg.Properties{WriteMetadataPathKey: "/w/meta"}, false},
		{"object storage layout", "", "/w/t", "/w/t/metadata/00001-a.metadata.json",
			iceberg.Properties{ObjectStoreEnabledKey: "true"}, false},
		{"location moved to another table", "", "/w/hot", "/w/t/metadata/00001-a.metadata.json", nil, false},
		{"location moved to the warehouse root", "", "/w", "/w/t/metadata/00001-a.metadata.json", nil, false},
		{"sibling sharing a prefix", "", "/w/t2", "/w/t/metadata/00001-a.metadata.json", nil, false},
		{"loaded from a subdirectory", "", "/w/t", "/w/t/metadata/x/00001-a.metadata.json", nil, false},
		{"another bucket", "", "s3://other/w/t", "s3://lake/w/t/metadata/00001-a.metadata.json", nil, false},
		{"relative loaded path with no base", "", "/w/t", "t/metadata/00001-a.metadata.json", nil, false},
		{"empty location", "", "", "/w/t/metadata/00001-a.metadata.json", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var meta Metadata
			if c.loc != "" {
				meta = newMeta(t, c.loc, c.props)
			} else {
				meta = emptyLocationMetadata(t, c.props)
			}
			err := CheckStrictLayout(c.base, meta, c.loaded)
			if c.ok {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, ErrStrictLayoutRefused)
				assert.ErrorIs(t, err, ErrMetadataWriteRefused, "a strict refusal is a permanent write refusal")
			}
		})
	}
	assert.NoError(t, CheckStrictLayout("", nil, "/w/t/metadata/00001-a.metadata.json"), "nil metadata passes")
}

// emptyLocationMetadata is metadata whose location field is empty, the shape a
// plant or a corrupt file can carry; NewMetadata would refuse to build it.
func emptyLocationMetadata(t *testing.T, props iceberg.Properties) Metadata {
	t.Helper()
	all := iceberg.Properties{PropertyFormatVersion: "2"}
	for k, v := range props {
		all[k] = v
	}
	meta, err := NewMetadata(iceberg.NewSchema(0), iceberg.UnpartitionedSpec, UnsortedSortOrder, "/w/t", all)
	require.NoError(t, err)
	bldr, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, bldr.SetLoc(""))
	out, err := bldr.Build()
	require.NoError(t, err)

	return out
}

func TestStrictLayoutCommit9633(t *testing.T) {
	t.Run("plain layout commits, the option follows the result", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		cat := &strictObservingCatalog{placingCatalog: f.cat}
		tbl, err := f.commitAs(t, true, cat)
		require.NoError(t, err)
		assert.Equal(t, 1, f.cat.attempts)
		assert.True(t, tbl.StrictLayout(), "Commit hands back a strict table")
		assert.Empty(t, cat.seen)
		assert.False(t, New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location, nil, f.cat).StrictLayout(), "off by default")
	})

	t.Run("the legitimate split layout: non-strict commits, strict refuses before the catalog", func(t *testing.T) {
		wh := filepath.ToSlash(t.TempDir())
		f := newAnchorFixture(t, iceberg.Properties{WriteMetadataPathKey: wh + "/meta-t"})
		f.cat.location = wh + "/meta-t/00001-t.metadata.json"
		touch(t, f.cat.location)

		_, err := f.commitAs(t, false, f.cat)
		require.NoError(t, err, "the write anchor accepts a split layout")
		require.Equal(t, 1, f.cat.attempts)

		f = newAnchorFixture(t, iceberg.Properties{WriteMetadataPathKey: wh + "/meta-t2"})
		f.cat.location = wh + "/meta-t2/00001-t.metadata.json"
		touch(t, f.cat.location)
		cat := &strictObservingCatalog{placingCatalog: f.cat}
		_, err = f.commitAs(t, true, cat)
		require.ErrorIs(t, err, ErrStrictLayoutRefused)
		assert.ErrorIs(t, err, ErrMetadataWriteRefused)
		assert.Zero(t, f.cat.attempts, "refused before CommitTable")
		require.Len(t, cat.seen, 1, "the catalog is told once")
		assert.ErrorIs(t, cat.seen[0], ErrStrictLayoutRefused)
	})

	t.Run("residual 1: data placed under another table while the metadata stays home", func(t *testing.T) {
		// The plant: location stays, write.data.path names the victim's data
		// directory. Metadata lands at home so the write anchor passes.
		f := newAnchorFixture(t, nil)
		f.withProps(t, iceberg.Properties{WriteDataPathKey: f.wh + "/hot/data"})
		_, err := f.commitAs(t, false, f.cat)
		require.NoError(t, err, "the write anchor alone cannot refuse data placement")
		require.Equal(t, 1, f.cat.attempts)

		f = newAnchorFixture(t, nil)
		f.withProps(t, iceberg.Properties{WriteDataPathKey: f.wh + "/hot/data"})
		cat := &strictObservingCatalog{placingCatalog: f.cat}
		_, err = f.commitAs(t, true, cat)
		require.ErrorIs(t, err, ErrStrictLayoutRefused)
		assert.Zero(t, f.cat.attempts)
		assert.Len(t, cat.seen, 1)
	})

	t.Run("object-storage layout is refused under strict", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		f.withProps(t, iceberg.Properties{ObjectStoreEnabledKey: "true"})
		_, err := f.commitAs(t, true, f.cat)
		require.ErrorIs(t, err, ErrStrictLayoutRefused)
		assert.Zero(t, f.cat.attempts)
	})

	t.Run("a moved location is refused by the anchor first, strict or not", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		f.plantInPlace(t, f.wh+"/hot")
		cat := &strictObservingCatalog{placingCatalog: f.cat}
		_, err := f.commitAs(t, true, cat)
		require.ErrorIs(t, err, ErrMetadataWriteRefused)
		assert.NotErrorIs(t, err, ErrStrictLayoutRefused, "the anchor's own refusal")
		assert.Zero(t, f.cat.attempts)
		assert.Len(t, cat.seen, 1)
	})
}

func TestStrictLayoutOrphanScan9633(t *testing.T) {
	load := func(t *testing.T, f anchorFixture, strict bool) *Table {
		t.Helper()
		tbl := New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location,
			func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, f.cat)
		if strict {
			tbl = tbl.WithStrictLayout()
		}

		return tbl
	}

	t.Run("plain layout scans its own location", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		_, err := load(t, f, true).DeleteOrphanFiles(t.Context(), WithDryRun(true))
		require.NoError(t, err)
	})

	t.Run("an explicit root outside the table is refused under strict only", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		_, err := load(t, f, false).DeleteOrphanFiles(t.Context(), WithDryRun(true), WithLocation(f.wh+"/hot"))
		require.NoError(t, err, "a caller may name any root today")

		_, err = load(t, f, true).DeleteOrphanFiles(t.Context(), WithDryRun(true), WithLocation(f.wh+"/hot"))
		require.ErrorIs(t, err, ErrStrictLayoutRefused)

		_, err = load(t, f, true).DeleteOrphanFiles(t.Context(), WithDryRun(true), WithLocation(f.loc+"/"))
		require.NoError(t, err, "the location itself, however spelled, is the table's own root")
	})

	t.Run("data placed elsewhere is refused under strict", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		f.withProps(t, iceberg.Properties{WriteDataPathKey: f.wh + "/hot/data"})
		_, err := load(t, f, false).DeleteOrphanFiles(t.Context(), WithDryRun(true))
		require.NoError(t, err, "the anchored root agrees, the scan runs today")

		_, err = load(t, f, true).DeleteOrphanFiles(t.Context(), WithDryRun(true))
		require.ErrorIs(t, err, ErrStrictLayoutRefused)
	})
}
