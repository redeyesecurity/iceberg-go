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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/avro/ocf"
)

// writeRootWithChildren writes a root manifest holding one inlined data entry and
// nChildren child records in its header, the way the producer does.
func writeRootWithChildren(t *testing.T, nChildren int, extra map[string][]byte) (data []byte, childrenJSON []byte) {
	t.Helper()
	sch := NewSchema(0, NestedField{ID: 1, Name: "x", Type: PrimitiveTypes.Int64})
	children := make([]ManifestFile, 0, nChildren)
	for i := range nChildren {
		children = append(children, NewManifestFile(2, fmt.Sprintf("s3://w/t/metadata/%08d-%08d-child-%040d.avro", i, i, i), 4096+int64(i), 0, 7).
			AddedFiles(3).ExistingFiles(2).AddedRows(30).ExistingRows(20).SequenceNum(int64(i)+1, int64(i)+1).Build())
	}
	var err error
	childrenJSON, err = EncodeRootChildren(children, "")
	require.NoError(t, err)
	inline := NewManifestFile(2, "", 0, 0, 7).AddedFiles(1).AddedRows(100).Build()
	inlineJSON, err := EncodeRootInline(inline)
	require.NoError(t, err)
	meta := map[string][]byte{
		RootManifestMarkerKey:   []byte("1"),
		RootManifestChildrenKey: childrenJSON,
		RootManifestInlineKey:   inlineJSON,
	}
	for k, v := range extra {
		meta[k] = v
	}
	var buf bytes.Buffer
	wr, err := NewManifestWriter(2, &buf, *UnpartitionedSpec, sch, 7,
		WithManifestWriterContent(ManifestContentData), WithManifestWriterMetadata(meta))
	require.NoError(t, err)
	b, err := NewDataFileBuilder(*UnpartitionedSpec, EntryContentData, "s3://w/t/data/one.parquet", ParquetFile,
		map[int]any{}, map[int]string{}, map[int]int{}, 100, 1000)
	require.NoError(t, err)
	require.NoError(t, wr.Add(NewManifestEntry(EntryStatusADDED, nil, nil, nil, b.Build())))
	require.NoError(t, wr.Close())

	return buf.Bytes(), childrenJSON
}

// A root whose child list is over the avro library's 1 MiB header cap (#4493) is
// still readable: the children come back from the header and the inlined entries
// from the body.
func TestRootManifestHeaderOverTheAvroCap(t *testing.T) {
	const n = 6000
	data, childrenJSON := writeRootWithChildren(t, n, nil)
	require.Greater(t, len(childrenJSON), ocfHeaderValueCap, "fixture must exceed the cap to prove anything")

	// The library alone refuses the file. If this ever passes, the cap was lifted
	// upstream and the tolerant reader can go.
	_, err := ocf.NewReader(bytes.NewReader(data))
	require.ErrorContains(t, err, "out of range")

	files, err := ReadRootOrManifestList(data, "s3://w/t/metadata/root.avro", "")
	require.NoError(t, err)
	require.Len(t, files, n+1)
	require.Equal(t, "s3://w/t/metadata/00000000-00000000-child-0000000000000000000000000000000000000000.avro", files[0].FilePath())
	inline := files[n]
	require.True(t, IsInlineRootManifest(inline))
	require.Equal(t, "s3://w/t/metadata/root.avro", inline.FilePath())

	entries, err := ReadManifest(inline, bytes.NewReader(data), false)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "s3://w/t/data/one.parquet", entries[0].DataFile().FilePath())
	require.Equal(t, int64(100), entries[0].DataFile().Count())
}

// A small root still goes through the same path.
func TestRootManifestHeaderUnderTheAvroCap(t *testing.T) {
	data, childrenJSON := writeRootWithChildren(t, 3, nil)
	require.Less(t, len(childrenJSON), ocfHeaderValueCap)
	files, err := ReadRootOrManifestList(data, "s3://w/t/metadata/root.avro", "")
	require.NoError(t, err)
	require.Len(t, files, 4)
	entries, err := ReadManifest(files[3], bytes.NewReader(data), false)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// Only caver keys may be dropped from the header handed to the library; an oversized
// standard key is a corrupt file.
func TestOCFReaderRefusesOversizedStandardKey(t *testing.T) {
	data, _ := writeRootWithChildren(t, 1, map[string][]byte{"iceberg.big": bytes.Repeat([]byte("x"), ocfHeaderValueCap+1)})
	_, _, err := newOCFReader(bytes.NewReader(data))
	require.ErrorContains(t, err, `"iceberg.big"`)
	require.ErrorContains(t, err, "over the")
}

// The parser round-trips a header byte for byte in the common case, so the library
// sees exactly what was written.
func TestOCFHeaderRoundTrip(t *testing.T) {
	data, _ := writeRootWithChildren(t, 2, nil)
	rd, meta, err := newOCFReader(bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, meta[RootManifestMarkerKey], rd.Metadata()[RootManifestMarkerKey])
	require.Equal(t, meta["format-version"], rd.Metadata()["format-version"])
	require.Equal(t, meta[RootManifestChildrenKey], rd.Metadata()[RootManifestChildrenKey])
}
