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

package iceberg

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	iceio "github.com/apache/iceberg-go/io"
)

// Root manifest (Iceberg v4 adaptive-metadata-tree proposal, in design upstream;
// caver-go#4443).
//
// A snapshot's entry point is normally a manifest LIST (one avro file naming N
// manifests) and every commit writes at least one new manifest plus a new list. With
// the table property table.RootManifestKey the entry point is a ROOT: a single avro
// file that IS a valid manifest (its records are the snapshot's INLINED data-file
// entries) and whose OCF header carries the list of CHILD manifests. A small commit
// therefore writes exactly one file. Readers need no new code path: Snapshot.Manifests
// returns the children as ordinary manifests plus one manifest record for the inlined
// section whose path is the root itself, so FetchEntries opens the root and reads its
// entries like any manifest.
//
// Header keys: RootManifestMarkerKey = "1"; RootManifestChildrenKey = JSON array of the
// child manifest records (paths relative to the FileIO base when relative paths are
// on); RootManifestInlineKey = JSON of the inline section's own manifest record
// (counts and partition summaries, computed before the root is written).

const (
	RootManifestMarkerKey   = "caver.root"
	RootManifestChildrenKey = "caver.root.children"
	RootManifestInlineKey   = "caver.root.inline"
)

// WithManifestWriterMetadata adds keys to the OCF header of the manifest being
// written (the root manifest's child list rides here).
func WithManifestWriterMetadata(extra map[string][]byte) ManifestWriterOption {
	return func(w *ManifestWriter) {
		if w.extraMeta == nil {
			w.extraMeta = map[string][]byte{}
		}
		for k, v := range extra {
			w.extraMeta[k] = v
		}
	}
}

// IsInlineRootManifest reports whether m is the in-memory record of a root
// manifest's inlined section (its path is the root file itself).
func IsInlineRootManifest(m ManifestFile) bool {
	mf, ok := m.(*manifestFile)

	return ok && mf.inlineRoot
}

// rootHeader is what a root manifest's OCF header decodes to.
type rootHeader struct {
	children []*manifestFile
	inline   *manifestFile
}

// readRootHeader peeks the OCF header of an opened manifest-list-or-root file. ok is
// false for an ordinary manifest list. The reader's position is undefined afterwards;
// callers re-seek.
func readRootHeader(r io.Reader) (_ *rootHeader, ok bool, err error) {
	// The header is parsed here rather than by the avro library: a root's child
	// list can exceed the library's 1 MiB per-value cap (ocf_header.go, #4493).
	h0, err := readOCFHeader(bufio.NewReader(r))
	if err != nil {
		return nil, false, err
	}
	meta := h0.meta
	if string(meta[RootManifestMarkerKey]) != "1" {
		return nil, false, nil
	}
	h := &rootHeader{}
	if raw := meta[RootManifestChildrenKey]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &h.children); err != nil {
			return nil, false, fmt.Errorf("root manifest: decode children: %w", err)
		}
	}
	if raw := meta[RootManifestInlineKey]; len(raw) > 0 {
		h.inline = &manifestFile{}
		if err := json.Unmarshal(raw, h.inline); err != nil {
			return nil, false, fmt.Errorf("root manifest: decode inline record: %w", err)
		}
	}

	return h, true, nil
}

// ReadRootOrManifestList reads a snapshot entry point that is either a manifest list
// or a root manifest, from its full bytes. rootPath is the absolute path of the file
// (the inline record's path). Manifest paths resolve against base.
func ReadRootOrManifestList(data []byte, rootPath, base string) ([]ManifestFile, error) {
	h, isRoot, err := readRootHeader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if !isRoot {
		return ReadManifestListWithBase(bytes.NewReader(data), base)
	}
	out := make([]ManifestFile, 0, len(h.children)+1)
	for _, c := range h.children {
		if c.version == 0 {
			c.version = 2
		}
		c.Path = iceio.JoinBase(base, c.Path)
		c.pathBase = base
		out = append(out, c)
	}
	if h.inline != nil && (h.inline.AddedFilesCount+h.inline.ExistingFilesCount+h.inline.DeletedFilesCount) > 0 {
		h.inline.Path = rootPath
		h.inline.pathBase = base
		h.inline.inlineRoot = true
		if h.inline.version == 0 {
			h.inline.version = 2
		}
		out = append(out, h.inline)
	}

	return out, nil
}

// AssignRootSequence returns m with an unassigned sequence number (and minimum) set
// to seq, the way the manifest list writer does for manifests the current commit
// wrote. Records that already carry a sequence are returned as they are.
func AssignRootSequence(m ManifestFile, seq int64) ManifestFile {
	mf, ok := m.(*manifestFile)
	if !ok {
		return m
	}
	if mf.SeqNumber != -1 && mf.MinSeqNumber != -1 {
		return m
	}
	cp := *mf
	if cp.SeqNumber == -1 {
		cp.SeqNumber = seq
	}
	if cp.MinSeqNumber == -1 {
		cp.MinSeqNumber = seq
	}

	return &cp
}

// EncodeRootChildren renders child manifest records for the root header, with paths
// relative to base.
func EncodeRootChildren(children []ManifestFile, base string) ([]byte, error) {
	recs := make([]manifestFile, 0, len(children))
	for _, c := range children {
		mf, ok := c.(*manifestFile)
		if !ok {
			return nil, fmt.Errorf("root manifest: child %s is not a manifest record", c.FilePath())
		}
		cp := *mf
		cp.Path = iceio.RelativizePath(base, cp.Path)
		recs = append(recs, cp)
	}

	return json.Marshal(recs)
}

// EncodeRootInline renders the inline section's manifest record for the root header.
func EncodeRootInline(inline ManifestFile) ([]byte, error) {
	mf, ok := inline.(*manifestFile)
	if !ok {
		return nil, fmt.Errorf("root manifest: inline record is not a manifest record")
	}
	cp := *mf
	cp.Path = ""

	return json.Marshal(cp)
}
