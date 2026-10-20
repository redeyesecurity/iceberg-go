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
	"strings"

	icebergio "github.com/apache/iceberg-go/io"
)

// ErrStrictLayoutRefused is returned by a commit or an orphan scan on a table
// built WithStrictLayout whose layout is not the plain one. It wraps
// ErrMetadataWriteRefused, so a caller that already treats that refusal as
// permanent (the RedEye lake catalog's commit classifier) needs no new case.
var ErrStrictLayoutRefused = fmt.Errorf("%w: strict layout", ErrMetadataWriteRefused)

// WithStrictLayout is t holding every commit and orphan scan to the plain table
// layout: the metadata file the table was loaded from sits in
// <location>/metadata, data files go under <location>/data, and none of
// write.data.path, write.metadata.path or write.object-storage.enabled moves
// either (caver-go#9633, option B).
//
// The write anchor (CheckMetadataWriteAnchor) holds the next metadata file to
// the directory the catalog pointer loaded from and accepts a split layout on
// purpose, metadata under write.metadata.path and data under the location. From
// the pointer alone that layout and a plant that moved the location onto another
// table while pointing write.metadata.path home are the same bytes, so the data
// placement of such a plant cannot be refused without a second source of truth.
// Strict layout is the cheap second source: a consumer whose tables never use a
// split layout (the RedEye lake catalog refuses one before the fork runs,
// caver-go#9428) opts in and the fork refuses the plant's shape itself. A table
// that needs a split layout does not opt in and keeps today's behaviour.
//
// Data files a writer placed before the commit are not removed by the refusal;
// the refusal keeps the metadata from landing and nothing is deleted. The
// returned table shares t's metadata, catalog and FileIO; transactions opened on
// it carry the option, and so do the tables Commit and Refresh hand back.
func (t Table) WithStrictLayout() *Table {
	t.strictLayout = true

	return &t
}

// StrictLayout reports whether t was built WithStrictLayout.
func (t Table) StrictLayout() bool { return t.strictLayout }

// CheckStrictLayout reports whether meta, loaded from loadedLoc, has the plain
// layout: no write.data.path, write.metadata.path or object-storage layout
// moving files away from <location>/data and <location>/metadata (a property
// that spells the default placement is allowed), and, when loadedLoc names a
// directory, that directory is <location>/metadata. fsBase is the FileIO base
// relative paths resolve against; "" for none. An empty loadedLoc or a bare file
// name (an in-memory table's placeholder) passes the directory part only; the
// property part always runs.
func CheckStrictLayout(fsBase string, meta Metadata, loadedLoc string) error {
	if meta == nil {
		return nil
	}
	loc := meta.Location()
	if loc == "" {
		return fmt.Errorf("%w: the table has no location", ErrStrictLayoutRefused)
	}
	props := meta.Properties()
	if props.GetBool(ObjectStoreEnabledKey, ObjectStoreEnabledDefault) {
		return fmt.Errorf("%w: %s is set", ErrStrictLayoutRefused, ObjectStoreEnabledKey)
	}
	locBase := icebergio.JoinBase(fsBase, loc)
	for _, p := range []struct{ key, sub string }{
		{WriteDataPathKey, "data"},
		{WriteMetadataPathKey, "metadata"},
	} {
		v, ok := props[p.key]
		if !ok {
			continue
		}
		want := strings.TrimRight(locBase, "/") + "/" + p.sub
		if !sameStorageDir(icebergio.JoinBase(fsBase, v), want) {
			return fmt.Errorf("%w: %s is %q, not %s/%s", ErrStrictLayoutRefused, p.key, v, loc, p.sub)
		}
	}
	if loadedLoc == "" || !strings.Contains(loadedLoc, "/") {
		return nil
	}
	loadedDir, ok := loadedMetadataDir(fsBase, loadedLoc)
	if !ok {
		return fmt.Errorf("%w: loaded metadata path %q has no directory", ErrStrictLayoutRefused, loadedLoc)
	}
	if !sameStorageDir(loadedDir, strings.TrimRight(locBase, "/")+"/metadata") {
		return fmt.Errorf("%w: loaded from %q, which is not %s/metadata", ErrStrictLayoutRefused, loadedDir, loc)
	}

	return nil
}
