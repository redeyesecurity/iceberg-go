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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

// Snapshot offloading (Iceberg v4 proposal under discussion; caver-go#4442).
//
// metadata.json carries every retained snapshot and the snapshot log, and it is
// rewritten on every commit, so commit cost grows with retained history. With the
// table property SnapshotsOffloadedKey="offloaded" the file written to disk keeps
// only the snapshots a reader needs to find the head (the current snapshot and every
// ref target) and points at an append-only chain of SEGMENT files under metadata/:
//
//	metadata/snapshots-<uuid>.json = {"parent": "<previous segment or empty>",
//	                                  "snapshots": [...new since parent...],
//	                                  "snapshot-log": [...new entries...]}
//
// A commit writes one small segment (the snapshots it added) and a metadata.json
// whose size no longer depends on history. Loading a table follows the chain and
// merges everything back, so in memory the Metadata is exactly what it would have
// been without offloading: the builder, expiry, time travel and every caller see the
// full list. The chain is folded into ONE segment when it grows past
// SnapshotsFoldEveryKey segments, and whenever the new metadata dropped snapshots
// the chain still holds (expiry), so a reader never walks more than the fold bound
// and never resurrects an expired snapshot.
//
// The pointer is a non-standard JSON key ("caver-snapshots-file"); standard readers
// ignore unknown keys and see a table whose history starts at its current snapshot.
// InlineSnapshots writes the standard form for export.

// SnapshotsOffloadedKey turns offloading on; the only recognised value is "offloaded".
const SnapshotsOffloadedKey = "caver.metadata.snapshots"

// SnapshotsOffloadedValue is the value of SnapshotsOffloadedKey that turns it on.
const SnapshotsOffloadedValue = "offloaded"

// SnapshotsFoldEveryKey bounds the segment chain: once it is this long the next write
// folds it into one segment. Default snapshotsFoldEveryDefault.
const SnapshotsFoldEveryKey = "caver.metadata.snapshots.fold-every"

const snapshotsFoldEveryDefault = 32

// snapshotSegment is one link of the chain.
type snapshotSegment struct {
	Parent      string             `json:"parent,omitempty"`
	Snapshots   []Snapshot         `json:"snapshots"`
	SnapshotLog []SnapshotLogEntry `json:"snapshot-log,omitempty"`
}

// SnapshotsOffloaded reports whether the metadata asks for offloading.
func SnapshotsOffloaded(m Metadata) bool {
	return m != nil && m.Properties().Get(SnapshotsOffloadedKey, "") == SnapshotsOffloadedValue
}

func snapshotsFoldEvery(props iceberg.Properties) int {
	if v, err := strconv.Atoi(strings.TrimSpace(props.Get(SnapshotsFoldEveryKey, ""))); err == nil && v > 0 {
		return v
	}

	return snapshotsFoldEveryDefault
}

// offloadedFile returns the segment pointer stored in the metadata, "" when none.
func offloadedFile(m Metadata) string {
	if c, ok := commonOf(m); ok {
		return c.SnapshotsFile
	}

	return ""
}

func commonOf(m Metadata) (*commonMetadata, bool) {
	switch v := m.(type) {
	case *metadataV1:
		return &v.commonMetadata, true
	case *metadataV2:
		return &v.commonMetadata, true
	case *metadataV3:
		return &v.commonMetadata, true
	}

	return nil, false
}

// readSegmentChain follows parents from head and returns every snapshot and log entry
// in the chain (oldest segment first) plus the chain length.
func readSegmentChain(fs iceio.IO, base, head string) (snaps []Snapshot, logs []SnapshotLogEntry, depth int, err error) {
	var segs []snapshotSegment
	seen := map[string]struct{}{}
	for p := head; p != ""; {
		if _, dup := seen[p]; dup {
			return nil, nil, 0, fmt.Errorf("snapshot segment chain loops at %s", p)
		}
		seen[p] = struct{}{}
		f, oerr := fs.Open(iceio.JoinBase(base, p))
		if oerr != nil {
			return nil, nil, 0, fmt.Errorf("open snapshot segment %s: %w", p, oerr)
		}
		var seg snapshotSegment
		derr := json.NewDecoder(f).Decode(&seg)
		f.Close()
		if derr != nil {
			return nil, nil, 0, fmt.Errorf("decode snapshot segment %s: %w", p, derr)
		}
		segs = append(segs, seg)
		p = seg.Parent
	}
	depth = len(segs)
	for i := len(segs) - 1; i >= 0; i-- {
		snaps = append(snaps, segs[i].Snapshots...)
		logs = append(logs, segs[i].SnapshotLog...)
	}

	return snaps, logs, depth, nil
}

// SegmentChainPaths returns every segment file in the chain headed by the metadata's
// pointer, resolved through fs (head first). Empty when the table is not offloaded.
// The orphan sweep uses it to keep the chain out of its candidate set.
func SegmentChainPaths(m Metadata, fs iceio.IO) ([]string, error) {
	head := offloadedFile(m)
	if head == "" || fs == nil {
		return nil, nil
	}
	base := iceio.PathBaseOf(fs)
	var out []string
	seen := map[string]struct{}{}
	for p := head; p != ""; {
		if _, dup := seen[p]; dup {
			return out, fmt.Errorf("snapshot segment chain loops at %s", p)
		}
		seen[p] = struct{}{}
		full := iceio.JoinBase(base, p)
		out = append(out, full)
		f, err := fs.Open(full)
		if err != nil {
			return out, fmt.Errorf("open snapshot segment %s: %w", p, err)
		}
		var seg snapshotSegment
		derr := json.NewDecoder(f).Decode(&seg)
		f.Close()
		if derr != nil {
			return out, fmt.Errorf("decode snapshot segment %s: %w", p, derr)
		}
		p = seg.Parent
	}

	return out, nil
}

// MergeOffloadedSnapshots loads the segment chain the metadata points at and merges it
// into the in-memory snapshot list and log, so callers see the full history. A
// metadata without a pointer is returned unchanged. The FileIO resolves relative
// segment paths (io.PathBaser) like every other path.
func MergeOffloadedSnapshots(m Metadata, fs iceio.IO) error {
	c, ok := commonOf(m)
	if !ok || c.SnapshotsFile == "" {
		return nil
	}
	if fs == nil {
		return errors.New("snapshot offloading: metadata points at a segment chain but no FileIO was given to read it")
	}
	snaps, logs, _, err := readSegmentChain(fs, iceio.PathBaseOf(fs), c.SnapshotsFile)
	if err != nil {
		return err
	}
	have := map[int64]struct{}{}
	for _, s := range c.SnapshotList {
		have[s.SnapshotID] = struct{}{}
	}
	merged := make([]Snapshot, 0, len(snaps)+len(c.SnapshotList))
	for _, s := range snaps {
		if _, dup := have[s.SnapshotID]; dup {
			continue
		}
		have[s.SnapshotID] = struct{}{}
		merged = append(merged, s)
	}
	merged = append(merged, c.SnapshotList...)
	slices.SortFunc(merged, func(a, b Snapshot) int {
		if a.SequenceNumber != b.SequenceNumber {
			return int(a.SequenceNumber - b.SequenceNumber)
		}

		return int(a.TimestampMs - b.TimestampMs)
	})
	c.SnapshotList = merged
	if len(logs) > 0 {
		seenLog := map[int64]struct{}{}
		var log []SnapshotLogEntry
		for _, e := range append(logs, c.SnapshotLog...) {
			key := e.SnapshotID*1_000_000_007 + e.TimestampMs
			if _, dup := seenLog[key]; dup {
				continue
			}
			seenLog[key] = struct{}{}
			log = append(log, e)
		}
		slices.SortFunc(log, func(a, b SnapshotLogEntry) int { return int(a.TimestampMs - b.TimestampMs) })
		c.SnapshotLog = log
	}

	return nil
}

// OffloadSnapshots prepares metadata for writing with offloading: it writes the next
// segment (or a folded one) through fs under the table's metadata directory and
// returns a COPY of the metadata whose snapshot list holds only the head snapshots
// and whose pointer names the new segment. The caller writes that copy as
// metadata.json. m itself is untouched (it stays the full in-memory view). When the
// property is off it returns m unchanged and writes nothing.
func OffloadSnapshots(m Metadata, fs iceio.WriteFileIO, metadataLocation string) (Metadata, error) {
	if !SnapshotsOffloaded(m) {
		return m, nil
	}
	c, ok := commonOf(m)
	if !ok {
		return m, nil
	}
	base := iceio.PathBaseOf(fs)
	// What the chain already holds.
	var chainSnaps []Snapshot
	var chainLogs []SnapshotLogEntry
	depth := 0
	if c.SnapshotsFile != "" {
		var err error
		chainSnaps, chainLogs, depth, err = readSegmentChain(fs, base, c.SnapshotsFile)
		if err != nil {
			return nil, err
		}
	}
	inChain := map[int64]struct{}{}
	for _, s := range chainSnaps {
		inChain[s.SnapshotID] = struct{}{}
	}
	live := map[int64]struct{}{}
	for _, s := range c.SnapshotList {
		live[s.SnapshotID] = struct{}{}
	}
	// Fold when the chain is long, or when it holds a snapshot the metadata no longer
	// has (expiry): a folded segment is the full live list with no parent.
	fold := depth >= snapshotsFoldEvery(c.Props)
	for id := range inChain {
		if _, still := live[id]; !still {
			fold = true

			break
		}
	}
	seg := snapshotSegment{}
	if fold || c.SnapshotsFile == "" {
		seg.Snapshots = append([]Snapshot(nil), c.SnapshotList...)
		seg.SnapshotLog = append([]SnapshotLogEntry(nil), c.SnapshotLog...)
	} else {
		seg.Parent = c.SnapshotsFile
		for _, s := range c.SnapshotList {
			if _, ok := inChain[s.SnapshotID]; !ok {
				seg.Snapshots = append(seg.Snapshots, s)
			}
		}
		chainLogKeys := map[int64]struct{}{}
		for _, e := range chainLogs {
			chainLogKeys[e.SnapshotID*1_000_000_007+e.TimestampMs] = struct{}{}
		}
		for _, e := range c.SnapshotLog {
			if _, ok := chainLogKeys[e.SnapshotID*1_000_000_007+e.TimestampMs]; !ok {
				seg.SnapshotLog = append(seg.SnapshotLog, e)
			}
		}
	}
	// String surgery, not path.Join: Join cleans "file:///w" down to "file:/w", which
	// no longer matches the base and would defeat both the relativize and the FileIO.
	dir := metadataLocation
	if i := strings.LastIndex(dir, "/"); i >= 0 {
		dir = dir[:i]
	}
	segPath := dir + "/snapshots-" + uuid.New().String() + ".json"
	out, err := fs.Create(segPath)
	if err != nil {
		return nil, fmt.Errorf("snapshot offloading: create segment: %w", err)
	}
	if err := json.NewEncoder(out).Encode(&seg); err != nil {
		out.Close()

		return nil, fmt.Errorf("snapshot offloading: write segment: %w", err)
	}
	if err := out.Close(); err != nil {
		return nil, err
	}

	// The written view: head snapshots only, pointer to the segment.
	keep := map[int64]struct{}{}
	if c.CurrentSnapshotID != nil {
		keep[*c.CurrentSnapshotID] = struct{}{}
	}
	for _, ref := range c.SnapshotRefs {
		keep[ref.SnapshotID] = struct{}{}
	}
	view, err := cloneMetadata(m)
	if err != nil {
		return nil, err
	}
	vc, _ := commonOf(view)
	vc.SnapshotList = vc.SnapshotList[:0:0]
	for _, s := range c.SnapshotList {
		if _, ok := keep[s.SnapshotID]; ok {
			vc.SnapshotList = append(vc.SnapshotList, s)
		}
	}
	if n := len(c.SnapshotLog); n > 0 {
		vc.SnapshotLog = []SnapshotLogEntry{c.SnapshotLog[n-1]}
	}
	vc.SnapshotsFile = iceio.RelativizePath(base, segPath)

	return view, nil
}

// InlineSnapshots returns a copy of m with the offload pointer cleared and the full
// snapshot list inline: the standard form, for export or for turning offloading off.
func InlineSnapshots(m Metadata) (Metadata, error) {
	view, err := cloneMetadata(m)
	if err != nil {
		return nil, err
	}
	if c, ok := commonOf(view); ok {
		c.SnapshotsFile = ""
	}

	return view, nil
}

// cloneMetadata deep-copies metadata through its JSON form (the only representation
// every version shares).
func cloneMetadata(m Metadata) (Metadata, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	out, err := ParseMetadataBytes(b)
	if err != nil {
		return nil, err
	}
	if c, ok := commonOf(out); ok {
		// ParseMetadataBytes keeps the pointer; the caller decides what to do with it.
		_ = c
	}

	return out, nil
}

// readAllClose is a small helper for tests and callers that hold an io.Reader.
func readAllClose(r io.ReadCloser) ([]byte, error) {
	defer r.Close()

	return io.ReadAll(r)
}
