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
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// containmentFixture is a table at <wh>/t whose catalog trims the whole
// metadata log on every commit, so every entry the base log names is handed to
// deleteOldMetadata.
type containmentFixture struct {
	wh, loc string
	cat     *oneVersionLogCatalog
}

func newContainmentFixture(t *testing.T) containmentFixture {
	t.Helper()
	wh := filepath.ToSlash(t.TempDir())
	loc := wh + "/t"
	require.NoError(t, os.MkdirAll(loc+"/metadata", 0o755))
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:               "2",
		MetadataDeleteAfterCommitEnabledKey: "true",
	})
	require.NoError(t, err)

	// The catalog pointer names a file in <loc>/metadata, the directory the
	// cleanup is anchored on.
	head := loc + "/metadata/current.metadata.json"
	require.NoError(t, os.WriteFile(head, []byte("{}"), 0o644))

	return containmentFixture{wh: wh, loc: loc, cat: &oneVersionLogCatalog{dir: loc, metadata: meta, location: head}}
}

// moveLocation rewrites the catalog's current metadata so its location field
// names newLoc, the other half of a plant that wants a wider delete scope.
func (f containmentFixture) moveLocation(t *testing.T, newLoc string) {
	t.Helper()
	bldr, err := MetadataBuilderFromBase(f.cat.metadata, "")
	require.NoError(t, err)
	require.NoError(t, bldr.SetLoc(newLoc))
	meta, err := bldr.Build()
	require.NoError(t, err)
	f.cat.metadata = meta
}

// plant rewrites the catalog's current metadata so its log names entries, the
// shape a writer who can rewrite metadata.json in place produces.
func (f containmentFixture) plant(t *testing.T, entries ...string) {
	t.Helper()
	bldr, err := MetadataBuilderFromBase(f.cat.metadata, "")
	require.NoError(t, err)
	for _, e := range entries {
		bldr.AppendMetadataLog(MetadataLogEntry{MetadataFile: e, TimestampMs: time.Now().UnixMilli()})
	}
	meta, err := bldr.Build()
	require.NoError(t, err)
	f.cat.metadata = meta
}

func touch(t *testing.T, p string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte("canary"), 0o644))
}

// commitOnce makes one ordinary commit through doCommit and returns the log.
func (f containmentFixture) commitOnce(t *testing.T, fsys iceio.IO) string {
	t.Helper()
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	tbl := New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location,
		func(context.Context) (iceio.IO, error) { return fsys, nil }, f.cat)
	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.NoError(t, err)

	return logBuf.String()
}

// cleanupOnce runs the old-metadata cleanup for a commit that trimmed the
// whole log of the catalog's current metadata, without going through
// doCommit, and returns the log.
func (f containmentFixture) cleanupOnce(t *testing.T, fsys iceio.IO) string {
	t.Helper()
	bldr, err := MetadataBuilderFromBase(f.cat.metadata, "")
	require.NoError(t, err)
	bldr.TrimMetadataLogs(0)
	newMeta, err := bldr.Build()
	require.NoError(t, err)

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)
	deleteOldMetadata(fsys, f.cat.metadata, f.cat.location, newMeta)

	return logBuf.String()
}

// The reviewer's probe (caver-go#9525): a metadata-log entry planted into
// metadata.json naming an object outside the table was deleted by the next
// ordinary commit. Every planted shape must survive the commit, the legitimate
// old file under <location>/metadata must still be deleted, and each refusal
// is logged with the path quoted.
func TestDeleteOldMetadataRefusesPlantedMetadataLogEntries9525(t *testing.T) {
	f := newContainmentFixture(t)

	legit := f.loc + "/metadata/v0.metadata.json"
	outside := f.wh + "/events_warm/canary-9525.json"
	outsideURI := f.wh + "/other/canary-uri-9525.json"
	sibling := f.wh + "/t2/metadata/canary-sibling-9525.json"
	sharedPrefix := f.loc + "/metadata-evil/canary-prefix-9525.json"
	dataFile := f.loc + "/data/canary-data-9525.parquet"
	dotdotTarget := f.wh + "/dotdot/canary-dotdot-9525.json"
	dotdot := f.loc + "/metadata/../../dotdot/canary-dotdot-9525.json"

	for _, p := range []string{legit, outside, outsideURI, sibling, sharedPrefix, dataFile, dotdotTarget} {
		touch(t, p)
	}
	planted := []string{outside, "file://" + outsideURI, sibling, sharedPrefix, dataFile, dotdot}
	f.plant(t, append(planted, legit)...)

	fsys := &removeRecordingIO{}
	out := f.commitOnce(t, fsys)

	for _, p := range []string{outside, outsideURI, sibling, sharedPrefix, dataFile, dotdotTarget} {
		assert.FileExists(t, p, "a planted metadata-log entry must survive the commit")
	}
	assert.NoFileExists(t, legit, "the legitimate old file in <location>/metadata is still deleted")
	assert.Equal(t, []string{legit}, fsys.removed, "only the contained entry reaches Remove")
	for _, p := range planted {
		assert.Contains(t, out, "Refused to delete old metadata file outside the table metadata directory: \""+p+"\"")
	}
	assert.NotContains(t, out, "Failed to delete old metadata file")
}

// A relative entry resolves against the FileIO base, the warehouse, exactly
// the way RelativeFS.Remove would open it, and is held to the same directory.
func TestDeleteOldMetadataResolvesRelativeEntriesAgainstTheFileIOBase9525(t *testing.T) {
	f := newContainmentFixture(t)

	legitAbs := f.loc + "/metadata/v0.metadata.json"
	outsideAbs := f.wh + "/events_warm/canary-rel-9525.json"
	dotdotAbs := f.wh + "/up/canary-rel-dotdot-9525.json"
	for _, p := range []string{legitAbs, outsideAbs, dotdotAbs} {
		touch(t, p)
	}
	f.plant(t, "events_warm/canary-rel-9525.json", "t/metadata/../../up/canary-rel-dotdot-9525.json",
		"t/metadata/v0.metadata.json")

	rec := &removeRecordingIO{}
	out := f.commitOnce(t, iceio.WrapRelative(rec, f.wh))

	assert.FileExists(t, outsideAbs)
	assert.FileExists(t, dotdotAbs)
	assert.NoFileExists(t, legitAbs, "a relative entry under <location>/metadata is deleted")
	assert.Equal(t, []string{legitAbs}, rec.removed, "the resolved path is what is removed")
	assert.Contains(t, out, `"events_warm/canary-rel-9525.json"`)
}

// The caver-go#9528 review probe (F1): a plant that moves the location field
// to another table's directory, or to the warehouse root, and names that
// table's head in the log. redeye.16 anchored on newMeta.Location(), which a
// catalog with nothing cached returns from the planted file, and the victim's
// head was deleted. The anchor is now the directory of the metadata file the
// writer loaded, so nothing outside <loc>/metadata is removed, and the
// table's own old file there still is.
//
// Since caver-go#9536 doCommit refuses a commit on such a base before the
// catalog sees it (metadata_write_anchor_test.go), so the cleanup is driven
// directly here: it stays a second, independent hold.
func TestALocationMovingPlantDeletesNothingOutsideTheLoadedDirectory9528(t *testing.T) {
	for _, tc := range []struct{ name, moveTo string }{
		{"another table", "/hot"},
		{"warehouse root", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newContainmentFixture(t)
			victimHead := f.wh + "/hot/metadata/00001-victim.metadata.json"
			rootMeta := f.wh + "/metadata/00001-root.metadata.json"
			legit := f.loc + "/metadata/v0.metadata.json"
			for _, p := range []string{victimHead, rootMeta, legit} {
				touch(t, p)
			}
			f.moveLocation(t, f.wh+tc.moveTo)
			f.plant(t, victimHead, rootMeta, legit)

			fsys := &removeRecordingIO{}
			out := f.cleanupOnce(t, fsys)

			assert.FileExists(t, victimHead, "the other table's head survives a location-moving plant")
			assert.FileExists(t, rootMeta, "the warehouse root's metadata survives a location-moving plant")
			assert.Equal(t, []string{legit}, fsys.removed, "only the entry in the loaded file's directory reaches Remove")
			assert.NoFileExists(t, legit)
			for _, p := range []string{victimHead, rootMeta} {
				assert.Contains(t, out, "Refused to delete old metadata file outside the table metadata directory: \""+p+"\"")
			}
			assert.Contains(t, out, `(metadata directory "`+f.loc+`/metadata")`)
		})
	}
}

// With no loaded metadata path there is no anchor, and each entry is refused.
func TestNoLoadedMetadataPathRefusesEachEntry9528(t *testing.T) {
	f := newContainmentFixture(t)
	legit := f.loc + "/metadata/v0.metadata.json"
	touch(t, legit)
	f.plant(t, legit)
	f.cat.location = ""

	fsys := &removeRecordingIO{}
	out := f.commitOnce(t, fsys)

	assert.Empty(t, fsys.removed)
	assert.FileExists(t, legit)
	assert.Contains(t, out, "Refused to delete old metadata file outside the table metadata directory: \""+legit+"\"")
}

func TestLoadedMetadataDir9528(t *testing.T) {
	cases := []struct {
		name, base, loc, want string
		ok                    bool
	}{
		{"local", "", "/w/t/metadata/v1.metadata.json", "/w/t/metadata", true},
		{"s3", "", "s3://lake/w/t/metadata/v1.metadata.json", "s3://lake/w/t/metadata", true},
		{"relative against base", "/w", "t/metadata/v1.metadata.json", "/w/t/metadata", true},
		{"relative with no base", "", "t/metadata/v1.metadata.json", "", false},
		{"empty", "", "", "", false},
		{"trailing slash", "", "/w/t/metadata/", "", false},
		{"no slash", "", "v1.metadata.json", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := loadedMetadataDir(tc.base, tc.loc)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataLogPathWithin9525(t *testing.T) {
	cases := []struct {
		name, base, loc, entry string
		want                   bool
	}{
		{"local inside", "", "/w/t", "/w/t/metadata/v1.metadata.json", true},
		{"file uri inside", "", "file:///w/t", "file:///w/t/metadata/v1.metadata.json", true},
		{"bare path against file uri location", "", "file:///w/t", "/w/t/metadata/v1.metadata.json", true},
		{"file: single slash", "", "/w/t", "file:/w/t/metadata/v1.metadata.json", true},
		{"nested under metadata", "", "/w/t", "/w/t/metadata/sub/v1.json", true},
		{"outside table", "", "/w/t", "/w/events_warm/canary.json", false},
		{"data dir", "", "/w/t", "/w/t/data/x.parquet", false},
		{"sibling table", "", "/w/t", "/w/t2/metadata/v1.json", false},
		{"shared string prefix", "", "/w/t", "/w/t/metadata-evil/v1.json", false},
		{"metadata dir itself", "", "/w/t", "/w/t/metadata", false},
		{"metadata dir slash", "", "/w/t", "/w/t/metadata/", false},
		{"dotdot", "", "/w/t", "/w/t/metadata/../../x.json", false},
		{"dot", "", "/w/t", "/w/t/metadata/./v1.json", false},
		{"encoded dotdot", "", "/w/t", "/w/t/metadata/%2e%2e/%2E%2E/x.json", false},
		{"encoded slash", "", "/w/t", "/w/t/metadata/..%2f..%2fx.json", false},
		{"empty segment", "", "/w/t", "/w/t/metadata//v1.json", false},
		{"backslash", "", "/w/t", "/w/t/metadata\\..\\..\\x.json", false},
		{"nul", "", "/w/t", "/w/t/metadata/v1\x00.json", false},
		{"newline", "", "/w/t", "/w/t/metadata/v1\n.json", false},
		{"file uri with host", "", "/w/t", "file://evil/w/t/metadata/v1.json", false},
		{"s3 on a local table", "", "/w/t", "s3://b/w/t/metadata/v1.json", false},
		{"relative with no base", "", "/w/t", "t/metadata/v1.json", false},
		{"relative with base", "/w", "/w/t", "t/metadata/v1.json", true},
		{"relative escaping", "/w", "/w/t", "t/metadata/../../x.json", false},
		{"relative other table", "/w", "/w/t", "t2/metadata/v1.json", false},
		{"s3 inside", "", "s3://lake/w/t", "s3://lake/w/t/metadata/v1.json", true},
		{"s3 scheme case", "", "s3://lake/w/t", "S3://lake/w/t/metadata/v1.json", true},
		{"s3 other bucket", "", "s3://lake/w/t", "s3://other/w/t/metadata/v1.json", false},
		{"s3 bucket case", "", "s3://lake/w/t", "s3://LAKE/w/t/metadata/v1.json", false},
		{"s3a on s3", "", "s3://lake/w/t", "s3a://lake/w/t/metadata/v1.json", false},
		{"gs on s3", "", "s3://lake/w/t", "gs://lake/w/t/metadata/v1.json", false},
		{"s3 Metadata case", "", "s3://lake/w/t", "s3://lake/w/t/Metadata/v1.json", false},
		{"s3 no bucket", "", "s3://lake/w/t", "s3:///w/t/metadata/v1.json", false},
		{"s3 query", "", "s3://lake/w/t", "s3://lake/w/t/metadata/v1.json?x=../../y", false},
		{"location trailing slash", "", "s3://lake/w/t/", "s3://lake/w/t/metadata/v1.json", true},
		{"empty entry", "", "/w/t", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, anchored := loadedMetadataDir(tc.base, strings.TrimRight(tc.loc, "/")+"/metadata/v9.metadata.json")
			require.True(t, anchored)
			got, ok := metadataLogPathWithin(tc.base, dir, tc.entry)
			assert.Equal(t, tc.want, ok, "entry %q", tc.entry)
			if ok {
				assert.Equal(t, iceio.JoinBase(tc.base, tc.entry), got)
			}
		})
	}
}
