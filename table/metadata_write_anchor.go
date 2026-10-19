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
	"errors"
	"fmt"
	"strings"
)

// Metadata write anchor (caver-go#9536).
//
// redeye.17 holds the old-metadata cleanup to the directory of the metadata
// file the writer loaded (metadata_log_containment.go), so one planted
// metadata.json cannot delete another table's head. The next metadata file,
// though, is placed by the location provider, which reads the location field
// and write.metadata.path of that same metadata.json. A plant that moves
// location to another table's directory therefore put the next head there,
// the catalog pointer followed it, and the commit after that was anchored on
// the victim's directory and deleted the victim's head from a planted log
// entry: two commits around the redeye.17 anchor.
//
// The hold: the base a writer loaded must place its metadata in the directory
// it was loaded from. A base whose own location and properties put the next
// file anywhere else is a plant, or a table this writer cannot place, and the
// commit is refused with ErrMetadataWriteRefused before the catalog writes
// anything. A commit that itself changes the location or write.metadata.path
// still moves its metadata: that is an update the caller sent, not one read
// from storage, and the catalog in front of it screens what it accepts.
//
// The same hold covers the data files (caver-go#9633). A plant that moves
// location AND sets write.metadata.path to the loaded directory passes the
// metadata check above, yet the writer places new data files from location
// (or write.data.path), so they landed under the victim's data directory. The
// loaded root is the parent of the loaded metadata directory, the layout every
// catalog here writes (<root>/metadata/<file>), and the base must place its
// data files strictly inside it.

// ErrMetadataWriteRefused is returned when a commit would write its new
// metadata file outside the directory of the metadata file it loaded.
var ErrMetadataWriteRefused = errors.New("new metadata would be written outside the loaded metadata file's directory")

// CheckMetadataWriteAnchor reports whether base, loaded from loadedLoc, places
// its next metadata file in loadedLoc's directory. fsBase is the FileIO base
// (io.PathBaseOf) that relative paths resolve against; "" for none.
//
// An empty loadedLoc passes: nothing was loaded from storage, so there is
// nothing a planted file could have moved. So does a bare file name with no
// directory part, the placeholder an in-memory table carries; a catalog
// pointer always names a directory. Any other loaded path or placement that
// does not resolve to an absolute directory is refused.
func CheckMetadataWriteAnchor(fsBase string, base Metadata, loadedLoc string) error {
	if loadedLoc == "" || !strings.Contains(loadedLoc, "/") || base == nil {
		return nil
	}
	loadedDir, ok := loadedMetadataDir(fsBase, loadedLoc)
	if !ok {
		return fmt.Errorf("%w: loaded metadata path %q has no directory to hold to", ErrMetadataWriteRefused, loadedLoc)
	}
	provider, err := LoadLocationProvider(base.Location(), base.Properties())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMetadataWriteRefused, err)
	}
	next := provider.NewMetadataLocation("next.metadata.json")
	nextDir, ok := loadedMetadataDir(fsBase, next)
	if !ok || !sameStorageDir(loadedDir, nextDir) {
		return fmt.Errorf("%w: loaded from %q, the base places the next file in %q", ErrMetadataWriteRefused, loadedDir, nextDir)
	}

	root := loadedDir[:strings.LastIndexByte(loadedDir, '/')+1]
	dataLoc := provider.NewDataLocation("anchor-probe.parquet")
	if _, ok := metadataLogPathWithin(fsBase, root, dataLoc); !ok {
		return fmt.Errorf("%w: loaded from %q, the base places data files at %q outside %q", ErrMetadataWriteRefused, loadedDir, dataLoc, root)
	}

	return nil
}

// sameStorageDir reports whether a and b name the same directory: same scheme
// and host, the same clean path. A path with an empty, "." or ".." segment is
// never the same as anything.
func sameStorageDir(a, b string) bool {
	aScheme, aHost, aPath, ok := splitStoragePath(a)
	if !ok {
		return false
	}
	bScheme, bHost, bPath, ok := splitStoragePath(b)
	if !ok {
		return false
	}
	if !cleanSegments(aPath) || !cleanSegments(bPath) {
		return false
	}

	return aScheme == bScheme && aHost == bHost &&
		strings.TrimRight(aPath, "/") == strings.TrimRight(bPath, "/")
}
