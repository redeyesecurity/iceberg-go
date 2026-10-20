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
	"errors"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// confirmingCatalog is the placing catalog plus a confirmed location: loc and
// ok are what ConfirmedTableLocation answers, err what it fails with.
type confirmingCatalog struct {
	*placingCatalog
	loc  string
	ok   bool
	err  error
	asks int
	seen []error
}

func (c *confirmingCatalog) ConfirmedTableLocation(context.Context, Identifier) (string, bool, error) {
	c.asks++

	return c.loc, c.ok, c.err
}

func (c *confirmingCatalog) ObserveCommitRefusal(_ context.Context, _ Identifier, err error) {
	c.seen = append(c.seen, err)
}

func (f anchorFixture) commitVia(t *testing.T, cat CatalogIO, updates ...Update) (*Table, error) {
	t.Helper()
	tbl := New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location,
		func(context.Context) (iceio.IO, error) { return &removeRecordingIO{}, nil }, cat)

	return tbl.doCommit(t.Context(), updates, nil)
}

func TestCheckConfirmedLocation9633(t *testing.T) {
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
		name, base, loc, confirmed string
		props                      iceberg.Properties
		ok                         bool
	}{
		{"agrees", "", "/w/t", "/w/t", nil, true},
		{"agrees, trailing slashes", "", "/w/t/", "/w/t", nil, true},
		{"agrees on s3", "", "s3://lake/w/t", "s3://lake/w/t", nil, true},
		{"relative location against the base", "/w", "t", "/w/t", nil, true},
		{"split layout under the confirmed location", "", "/w/t", "/w/t",
			iceberg.Properties{WriteMetadataPathKey: "/w/t/meta2", WriteDataPathKey: "/w/t/d"}, true},
		{"object-storage layout under the confirmed location", "", "/w/t", "/w/t",
			iceberg.Properties{ObjectStoreEnabledKey: "true"}, true},
		{"empty confirmed location passes", "", "/w/hot", "", nil, true},

		{"location moved to another table", "", "/w/hot", "/w/t", nil, false},
		{"location moved, both placements pointed back home", "", "/w/hot", "/w/t",
			iceberg.Properties{WriteDataPathKey: "/w/t/data", WriteMetadataPathKey: "/w/t/metadata"}, false},
		{"location moved to the warehouse root", "", "/w", "/w/t", nil, false},
		{"location moved to a prefix sibling", "", "/w/t2", "/w/t", nil, false},
		{"empty location", "", "", "/w/t", nil, false},
		{"another bucket", "", "s3://other/w/t", "s3://lake/w/t", nil, false},
		{"write.data.path on another table", "", "/w/t", "/w/t", iceberg.Properties{WriteDataPathKey: "/w/hot/data"}, false},
		{"write.data.path on a prefix sibling", "", "/w/t", "/w/t", iceberg.Properties{WriteDataPathKey: "/w/t2/data"}, false},
		{"write.metadata.path on another table", "", "/w/t", "/w/t", iceberg.Properties{WriteMetadataPathKey: "/w/hot/metadata"}, false},
		{"write.data.path climbing out", "", "/w/t", "/w/t", iceberg.Properties{WriteDataPathKey: "/w/t/../hot/data"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var meta Metadata
			if c.loc != "" {
				meta = newMeta(t, c.loc, c.props)
			} else {
				meta = emptyLocationMetadata(t, c.props)
			}
			err := CheckConfirmedLocation(c.base, meta, c.confirmed)
			if c.ok {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, ErrConfirmedLocationRefused)
				assert.ErrorIs(t, err, ErrMetadataWriteRefused, "a confirmed-location refusal is a permanent write refusal")
			}
		})
	}
	assert.NoError(t, CheckConfirmedLocation("", nil, "/w/t"), "nil metadata passes")
}

func TestConfirmedLocationCommit9633(t *testing.T) {
	t.Run("a table that agrees commits, the catalog is asked once per attempt", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		cat := &confirmingCatalog{placingCatalog: f.cat, loc: f.loc, ok: true}
		_, err := f.commitVia(t, cat)
		require.NoError(t, err)
		assert.Equal(t, 1, f.cat.attempts)
		assert.Equal(t, 1, cat.asks)
		assert.Empty(t, cat.seen)
	})

	t.Run("the split plant: location moved onto the victim, write.metadata.path home", func(t *testing.T) {
		// The write anchor passes this (the next metadata file lands in the
		// loaded directory). Only the confirmed location can refuse it.
		f := newAnchorFixture(t, nil)
		f.plantInPlace(t, f.wh+"/hot")
		f.withProps(t, iceberg.Properties{WriteMetadataPathKey: f.loc + "/metadata"})
		_, err := f.commitVia(t, f.cat)
		require.NoError(t, err, "without a confirmed location the anchor accepts the split plant")
		require.Equal(t, 1, f.cat.attempts)

		f = newAnchorFixture(t, nil)
		f.plantInPlace(t, f.wh+"/hot")
		f.withProps(t, iceberg.Properties{WriteMetadataPathKey: f.loc + "/metadata"})
		cat := &confirmingCatalog{placingCatalog: f.cat, loc: f.loc, ok: true}
		_, err = f.commitVia(t, cat)
		require.ErrorIs(t, err, ErrConfirmedLocationRefused)
		assert.ErrorIs(t, err, ErrMetadataWriteRefused)
		assert.Zero(t, f.cat.attempts, "refused before CommitTable")
		require.Len(t, cat.seen, 1, "the catalog is told once")
		assert.ErrorIs(t, cat.seen[0], ErrConfirmedLocationRefused)
	})

	t.Run("the location moved with both placements pointed home is still refused", func(t *testing.T) {
		// Nothing lands outside the confirmed location, but the location field
		// itself names the victim, and readers scope on that field.
		f := newAnchorFixture(t, nil)
		f.plantInPlace(t, f.wh+"/hot")
		f.withProps(t, iceberg.Properties{WriteDataPathKey: f.loc + "/data", WriteMetadataPathKey: f.loc + "/metadata"})
		cat := &confirmingCatalog{placingCatalog: f.cat, loc: f.loc, ok: true}
		_, err := f.commitVia(t, cat)
		require.ErrorIs(t, err, ErrConfirmedLocationRefused)
		assert.Contains(t, err.Error(), "names location")
		assert.Zero(t, f.cat.attempts)
	})

	t.Run("residual 1: write.data.path on the victim with the location home", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		f.withProps(t, iceberg.Properties{WriteDataPathKey: f.wh + "/hot/data"})
		cat := &confirmingCatalog{placingCatalog: f.cat, loc: f.loc, ok: true}
		_, err := f.commitVia(t, cat)
		require.ErrorIs(t, err, ErrConfirmedLocationRefused)
		assert.Zero(t, f.cat.attempts)
	})

	t.Run("a legitimate split layout under the confirmed location commits", func(t *testing.T) {
		wh := filepath.ToSlash(t.TempDir())
		f := newAnchorFixture(t, iceberg.Properties{WriteMetadataPathKey: wh + "/t/meta2"})
		f.cat.metadata = plantedMetadata(t, f.cat.metadata, wh+"/t")
		f.cat.location = wh + "/t/meta2/00001-t.metadata.json"
		touch(t, f.cat.location)
		cat := &confirmingCatalog{placingCatalog: f.cat, loc: wh + "/t", ok: true}
		_, err := f.commitVia(t, cat)
		require.NoError(t, err)
		assert.Equal(t, 1, f.cat.attempts)
	})

	t.Run("no confirmed location: behaviour unchanged", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		f.withProps(t, iceberg.Properties{WriteDataPathKey: f.wh + "/hot/data"})
		cat := &confirmingCatalog{placingCatalog: f.cat, ok: false}
		_, err := f.commitVia(t, cat)
		require.NoError(t, err, "ok=false leaves the anchor as the only check")
		assert.Equal(t, 1, cat.asks)
		assert.Equal(t, 1, f.cat.attempts)
	})

	t.Run("a catalog that cannot be asked ends the commit with its error, not a refusal", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		boom := errors.New("pointer store unreachable")
		cat := &confirmingCatalog{placingCatalog: f.cat, err: boom}
		_, err := f.commitVia(t, cat)
		require.ErrorIs(t, err, boom)
		assert.NotErrorIs(t, err, ErrMetadataWriteRefused)
		assert.Zero(t, f.cat.attempts)
		assert.Empty(t, cat.seen)
	})

	t.Run("asked again on a retry, against the refreshed base", func(t *testing.T) {
		// The first attempt loses the pointer race; the base it refreshes to
		// carries a write.data.path plant. The second attempt asks the catalog
		// again and is refused on that base, with one CommitTable attempt made.
		f := newAnchorFixture(t, iceberg.Properties{
			CommitNumRetriesKey:     "2",
			CommitMinRetryWaitMsKey: "1",
			CommitMaxRetryWaitMsKey: "2",
		})
		f.cat.failFirst = 1
		victimData := f.wh + "/hot/data"
		f.cat.onFail = func(c *placingCatalog) {
			bldr, err := MetadataBuilderFromBase(c.metadata, "")
			require.NoError(t, err)
			require.NoError(t, bldr.SetProperties(iceberg.Properties{WriteDataPathKey: victimData}))
			c.metadata, err = bldr.Build()
			require.NoError(t, err)
		}
		cat := &confirmingCatalog{placingCatalog: f.cat, loc: f.loc, ok: true}
		_, err := f.commitVia(t, cat)
		require.ErrorIs(t, err, ErrConfirmedLocationRefused)
		assert.Equal(t, 1, f.cat.attempts, "the lost race, then the refusal before a second CommitTable")
		assert.Equal(t, 2, cat.asks, "asked once per attempt")
		require.Len(t, cat.seen, 1)
	})
}

func TestConfirmedLocationOrphanScan9633(t *testing.T) {
	load := func(f anchorFixture, cat CatalogIO) *Table {
		return New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location,
			func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	}

	t.Run("agrees: the anchored root scans", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		cat := &confirmingCatalog{placingCatalog: f.cat, loc: f.loc, ok: true}
		_, err := load(f, cat).DeleteOrphanFiles(t.Context(), WithDryRun(true))
		require.NoError(t, err)
		assert.Equal(t, 1, cat.asks)
	})

	t.Run("a caller-named root outside the confirmed location is refused", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		cat := &confirmingCatalog{placingCatalog: f.cat, loc: f.loc, ok: true}
		_, err := load(f, cat).DeleteOrphanFiles(t.Context(), WithDryRun(true), WithLocation(f.wh+"/hot"))
		require.ErrorIs(t, err, ErrConfirmedLocationRefused)

		_, err = load(f, cat).DeleteOrphanFiles(t.Context(), WithDryRun(true), WithLocation(f.loc+"/data"))
		require.NoError(t, err, "a root under the confirmed location is the table's own")
	})

	t.Run("R1: the warehouse-rooted shape, confirmed location says otherwise", func(t *testing.T) {
		// Pointer at <wh>/metadata, location planted to the warehouse root: the
		// #24 anchor agrees (it is what a legitimate warehouse-rooted table
		// looks like). The catalog confirmed <wh>/t, so the scan is refused.
		f := newAnchorFixture(t, nil)
		f.cat.location = f.wh + "/metadata/00001-t.metadata.json"
		touch(t, f.cat.location)
		f.plantInPlace(t, f.wh)
		_, err := load(f, f.cat).DeleteOrphanFiles(t.Context(), WithDryRun(true))
		require.NoError(t, err, "from the pointer alone this is a legitimate warehouse-rooted table")

		cat := &confirmingCatalog{placingCatalog: f.cat, loc: f.loc, ok: true}
		_, err = load(f, cat).DeleteOrphanFiles(t.Context(), WithDryRun(true))
		require.ErrorIs(t, err, ErrConfirmedLocationRefused)
	})

	t.Run("a catalog that cannot be asked ends the scan with its error", func(t *testing.T) {
		f := newAnchorFixture(t, nil)
		boom := errors.New("pointer store unreachable")
		cat := &confirmingCatalog{placingCatalog: f.cat, err: boom}
		_, err := load(f, cat).DeleteOrphanFiles(t.Context(), WithDryRun(true))
		require.ErrorIs(t, err, boom)
	})
}
