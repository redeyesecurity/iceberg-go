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


package internal_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingObserver struct {
	rows    int64
	batches int
	path    string
}

func (o *countingObserver) Observe(b arrow.RecordBatch) { o.rows += b.NumRows(); o.batches++ }

func (o *countingObserver) KeyValues() map[string]string {
	return map[string]string{"z.rows": strconv.FormatInt(o.rows, 10), "a.path": o.path}
}

func observerFixture(t *testing.T) (*arrow.Schema, *iceberg.Schema, []arrow.RecordBatch) {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{{
		Name: "v", Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"1"}),
	}}, nil)
	var recs []arrow.RecordBatch
	for _, n := range []int{3, 4} {
		b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
		for i := 0; i < n; i++ {
			b.Field(0).(*array.StringBuilder).Append("x")
		}
		recs = append(recs, b.NewRecordBatch())
		b.Release()
	}
	ice, err := table.ArrowSchemaToIceberg(sc, false, nil)
	require.NoError(t, err)

	return sc, ice, recs
}

var observerStats = map[int]internal.StatisticsCollector{1: {
	FieldID: 1, IcebergTyp: iceberg.PrimitiveTypes.String, ColName: "v",
	Mode: internal.MetricsMode{Typ: internal.MetricModeFull},
}}

func footerKV(t *testing.T, path string) map[string]string {
	t.Helper()
	rdr, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer rdr.Close()
	out := map[string]string{}
	kv := rdr.MetaData().KeyValueMetadata()
	for i, k := range kv.Keys() {
		out[k] = kv.Values()[i]
	}

	return out
}

// The observer sees every row of every batch and its pairs land in the footer of the
// SAME file; a write without an observer writes no such pairs.
func TestFileObserverWritesFooterKeyValues(t *testing.T) {
	sc, ice, recs := observerFixture(t)
	fm := internal.GetFileFormat(iceberg.ParquetFile)
	dir := t.TempDir()

	obs := &countingObserver{}
	var gotContent iceberg.ManifestEntryContent = -1
	ctx := table.WithFileObserver(context.Background(), func(path string, content iceberg.ManifestEntryContent, schema *arrow.Schema) table.FileObserver {
		obs.path = path
		gotContent = content
		assert.True(t, schema.Equal(sc))

		return obs
	})
	observed := filepath.Join(dir, "observed.parquet")
	_, err := fm.WriteDataFile(ctx, iceio.LocalFS{}, nil, internal.WriteFileInfo{
		FileSchema: ice, FileName: observed, StatsCols: observerStats, WriteProps: []parquet.WriterProperty{}, Content: iceberg.EntryContentData,
	}, recs)
	require.NoError(t, err)
	assert.Equal(t, int64(7), obs.rows)
	assert.Equal(t, 2, obs.batches)
	assert.Equal(t, iceberg.EntryContentData, gotContent)
	kv := footerKV(t, observed)
	assert.Equal(t, "7", kv["z.rows"])
	assert.Equal(t, observed, kv["a.path"])

	plain := filepath.Join(dir, "plain.parquet")
	_, err = fm.WriteDataFile(context.Background(), iceio.LocalFS{}, nil, internal.WriteFileInfo{
		FileSchema: ice, FileName: plain, StatsCols: observerStats, WriteProps: []parquet.WriterProperty{},
	}, recs)
	require.NoError(t, err)
	kv = footerKV(t, plain)
	assert.NotContains(t, kv, "z.rows")
	assert.NotContains(t, kv, "a.path")
}

// A factory that returns nil declines the file: it is written exactly as without one.
func TestFileObserverFactoryMayDecline(t *testing.T) {
	_, ice, recs := observerFixture(t)
	fm := internal.GetFileFormat(iceberg.ParquetFile)
	ctx := table.WithFileObserver(context.Background(), func(string, iceberg.ManifestEntryContent, *arrow.Schema) table.FileObserver { return nil })
	p := filepath.Join(t.TempDir(), "declined.parquet")
	_, err := fm.WriteDataFile(ctx, iceio.LocalFS{}, nil, internal.WriteFileInfo{FileSchema: ice, FileName: p, StatsCols: observerStats, WriteProps: []parquet.WriterProperty{}}, recs)
	require.NoError(t, err)
	assert.NotContains(t, footerKV(t, p), "z.rows")
}
