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
	"strings"

	icebergio "github.com/apache/iceberg-go/io"
)

// Metadata-log containment (caver-go#9525).
//
// deleteOldMetadata removes the metadata-log entries a commit trims, and those
// entries are strings read from the base metadata.json. A writer who can
// rewrite metadata.json in place can plant an entry naming any object the
// FileIO reaches, and before this check the next ordinary commit by anyone
// deleted it. Each entry is now held to the directory of the metadata file the
// writer loaded, the path the catalog's pointer named, before it is removed;
// anything else is left in place and logged.
//
// The anchor is that loaded path, not the location field of the parsed
// metadata. redeye.16 anchored on newMeta.Location(), and a catalog with
// nothing cached returns the planted file's own location field, so a plant
// that also moved location to another table's directory reached that
// table's head (caver-go#9528 review, F1). The pointer path is not something
// the file's contents can move.
//
// The check resolves an entry exactly the way the delete does: a relative
// entry joins the FileIO's base (the warehouse under write.paths.relative),
// not the table location, because that is what RelativeFS.Remove would open.
// The resolved absolute path is what gets removed, so the string that was
// checked is the string that is deleted.
//
// With no loaded path (a table built with an empty metadata location) there is
// no anchor and each entry is refused. That fails safe: a leak, not a wrong
// delete.

// loadedMetadataDir returns the directory of the metadata file the writer
// loaded, resolved against the FileIO base, the only directory a
// metadata-log delete may touch. It reports false when there is no usable
// anchor.
func loadedMetadataDir(fsBase, metadataLocation string) (string, bool) {
	if metadataLocation == "" {
		return "", false
	}
	resolved := icebergio.JoinBase(fsBase, metadataLocation)
	if icebergio.IsRelativePath(resolved) {
		return "", false
	}
	i := strings.LastIndexByte(resolved, '/')
	if i <= 0 || i == len(resolved)-1 {
		return "", false
	}

	return resolved[:i], true
}

// metadataLogPathWithin resolves entry against the FileIO base and reports
// whether the result names a file strictly inside metaDir. It returns the
// resolved path to delete.
func metadataLogPathWithin(fsBase, metaDir, entry string) (string, bool) {
	if entry == "" || icebergio.IsRelativePath(metaDir) {
		return "", false
	}
	if icebergio.IsRelativePath(entry) && fsBase == "" {
		// Nothing says where a relative entry would resolve: the delete
		// would open it against the working directory.
		return "", false
	}
	resolved := icebergio.JoinBase(fsBase, entry)

	dScheme, dHost, dPath, ok := splitStoragePath(metaDir)
	if !ok {
		return "", false
	}
	eScheme, eHost, ePath, ok := splitStoragePath(resolved)
	if !ok {
		return "", false
	}
	if eScheme != dScheme || eHost != dHost {
		return "", false
	}
	if !cleanSegments(ePath) || !cleanSegments(dPath) {
		return "", false
	}
	prefix := strings.TrimRight(dPath, "/") + "/"
	if !strings.HasPrefix(ePath, prefix) || len(ePath) == len(prefix) {
		return "", false
	}

	return resolved, true
}

// splitStoragePath splits an absolute path into a normalised scheme, the
// bucket or host, and the path. A bare "/x" and "file:/x" are the local
// scheme with no host. A file URI naming a host is refused: LocalFS would
// not honour it the way the string reads.
func splitStoragePath(p string) (scheme, host, rest string, ok bool) {
	for i := 0; i < len(p); i++ {
		if c := p[i]; c < 0x20 || c == 0x7f || c == '\\' || c == '?' || c == '#' {
			return "", "", "", false
		}
	}
	switch {
	case strings.HasPrefix(p, "/"):
		return "file", "", p, true
	case strings.Contains(p, "://"):
		s, after, _ := strings.Cut(p, "://")
		scheme = strings.ToLower(s)
		host, rest, found := strings.Cut(after, "/")
		if !found || scheme == "" || strings.Contains(scheme, "/") {
			return "", "", "", false
		}
		if scheme == "file" && host != "" {
			return "", "", "", false
		}
		if scheme != "file" && host == "" {
			return "", "", "", false
		}

		return scheme, host, "/" + rest, true
	case strings.HasPrefix(strings.ToLower(p), "file:/"):
		return "file", "", p[len("file:"):], true
	}

	return "", "", "", false
}

// cleanSegments reports whether p has no empty, "." or ".." segment, spelled
// plainly or percent-encoded. Such a path is refused rather than cleaned: a
// storage key is not a file system path, and an object store keeps
// "metadata/../x" as a literal key while LocalFS walks it upward.
func cleanSegments(p string) bool {
	segs := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for i, seg := range segs {
		if seg == "" && i == len(segs)-1 {
			continue
		}
		low := strings.ToLower(strings.ReplaceAll(strings.ToLower(seg), "%2e", "."))
		if seg == "" || low == "." || low == ".." || strings.Contains(strings.ToLower(seg), "%2f") {
			return false
		}
	}

	return true
}
