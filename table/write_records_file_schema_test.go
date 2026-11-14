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

package table_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// RedEye fork (PATCH.md section 6, caver-go#8012): WithRecordFileSchema writes a data
// file that declares only the columns the records carry.

// wideTableSchema is a table with more columns than a batch uses: two top-level
// columns the batch leaves out, and a struct of which the batch fills one child.
func wideTableSchema() *iceberg.Schema {
	return iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "meta", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 4, Name: "a", Type: iceberg.PrimitiveTypes.String},
			{ID: 5, Name: "b", Type: iceberg.PrimitiveTypes.String},
			{ID: 6, Name: "c", Type: iceberg.PrimitiveTypes.Int64},
		}}},
		iceberg.NestedField{ID: 7, Name: "note", Type: iceberg.PrimitiveTypes.String},
	)
}

func wideTable(t *testing.T, sch *iceberg.Schema, spec iceberg.PartitionSpec) *table.Table {
	t.Helper()
	loc := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(sch, &spec, table.UnsortedSortOrder, loc, iceberg.Properties{})
	require.NoError(t, err)

	return table.New(table.Identifier{"test", "wide"}, meta, filepath.Join(loc, "metadata", "v1.metadata.json"),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil)
}

// narrowBatch carries id (as int32, which the table widens to int64) and meta.a only.
func narrowBatch(rows int) (*arrow.Schema, arrow.RecordBatch) {
	metaType := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.BinaryTypes.String, Nullable: true})
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "meta", Type: metaType, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	for i := range rows {
		b.Field(0).(*array.Int32Builder).Append(int32(i))
		sb := b.Field(1).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.StringBuilder).Append("v")
	}

	return sc, b.NewRecordBatch()
}

func writeOne(t *testing.T, tbl *table.Table, sc *arrow.Schema, rec arrow.RecordBatch, opts ...table.WriteRecordOption) (iceberg.DataFile, error) {
	t.Helper()
	records := func(yield func(arrow.RecordBatch, error) bool) { yield(rec, nil) }
	var out iceberg.DataFile
	for df, err := range table.WriteRecords(context.Background(), tbl, sc, records, opts...) {
		if err != nil {
			return nil, err
		}
		require.Nil(t, out, "more than one data file")
		out = df
	}
	require.NotNil(t, out, "no data file")

	return out, nil
}

// leavesOf returns each leaf column's dotted path and the parquet field id on it.
func leavesOf(t *testing.T, df iceberg.DataFile) (paths []string, ids []int32, size int64) {
	t.Helper()
	p := strings.TrimPrefix(df.FilePath(), "file://")
	st, err := os.Stat(p)
	require.NoError(t, err)
	rdr, err := file.OpenParquetFile(p, false)
	require.NoError(t, err)
	defer rdr.Close()
	sc := rdr.MetaData().Schema
	for i := range sc.NumColumns() {
		col := sc.Column(i)
		paths = append(paths, col.Path())
		ids = append(ids, col.SchemaNode().FieldID())
	}

	return paths, ids, st.Size()
}

func TestWithRecordFileSchemaWritesOnlyTheColumnsTheRecordsCarry(t *testing.T) {
	tbl := wideTable(t, wideTableSchema(), iceberg.NewPartitionSpec())

	sc, rec := narrowBatch(200)
	whole, err := writeOne(t, tbl, sc, rec)
	require.NoError(t, err)
	paths, _, wholeSize := leavesOf(t, whole)
	require.Equal(t, []string{"id", "name", "meta.a", "meta.b", "meta.c", "note"}, paths,
		"without the option the file declares every column of the table")

	sc, rec = narrowBatch(200)
	narrow, err := writeOne(t, tbl, sc, rec, table.WithRecordFileSchema())
	require.NoError(t, err)
	paths, ids, narrowSize := leavesOf(t, narrow)
	require.Equal(t, []string{"id", "meta.a"}, paths)
	require.Equal(t, []int32{1, 4}, ids, "the kept columns carry the table's field ids")
	require.Equal(t, int64(200), narrow.Count())
	require.Less(t, narrowSize, wholeSize)

	// The file's types are the table's, not the batch's: id was int32 in the batch.
	rdr, err := file.OpenParquetFile(strings.TrimPrefix(narrow.FilePath(), "file://"), false)
	require.NoError(t, err)
	defer rdr.Close()
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	got, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	defer got.Release()
	require.Equal(t, arrow.INT64, got.Schema().Field(0).Type.ID())

	// A reader that resolves by field id sees the omitted columns as null, and the
	// values that were written unchanged.
	fileSchema, err := table.ArrowSchemaToIceberg(got.Schema(), false, nil)
	require.NoError(t, err)
	tr := array.NewTableReader(got, got.NumRows())
	defer tr.Release()
	require.True(t, tr.Next())
	full, err := table.ToRequestedSchema(context.Background(), wideTableSchema(), fileSchema, tr.RecordBatch(),
		table.SchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)
	defer full.Release()
	require.Equal(t, int64(4), full.NumCols())
	require.Equal(t, int64(200), full.NumRows())
	id := full.Column(0).(*array.Int64)
	meta := full.Column(2).(*array.Struct)
	require.Equal(t, 3, meta.NumField())
	for i := range 200 {
		require.Equal(t, int64(i), id.Value(i))
		require.Equal(t, "v", meta.Field(0).(*array.String).Value(i))
	}
	require.Equal(t, 200, full.Column(1).NullN(), "name was not in the file")
	require.Equal(t, 200, meta.Field(1).NullN(), "meta.b was not in the file")
	require.Equal(t, 200, meta.Field(2).NullN(), "meta.c was not in the file")
	require.Equal(t, 200, full.Column(3).NullN(), "note was not in the file")
}

func TestWithRecordFileSchemaRefusesToOmitAPartitionSourceColumn(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "name", Transform: iceberg.IdentityTransform{},
	})
	tbl := wideTable(t, wideTableSchema(), spec)
	sc, rec := narrowBatch(10)
	_, err := writeOne(t, tbl, sc, rec, table.WithRecordFileSchema())
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	require.ErrorContains(t, err, `partition field "name"`)
}

func TestWithRecordFileSchemaStillRefusesAMissingRequiredColumn(t *testing.T) {
	sch := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "meta", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 4, Name: "a", Type: iceberg.PrimitiveTypes.String},
		}}},
	)
	tbl := wideTable(t, sch, iceberg.NewPartitionSpec())
	sc, rec := narrowBatch(10)
	defer rec.Release()
	records := func(yield func(arrow.RecordBatch, error) bool) {}
	var got error
	for _, err := range table.WriteRecords(context.Background(), tbl, sc, records, table.WithRecordFileSchema()) {
		got = err
	}
	require.Error(t, got)
	require.ErrorContains(t, got, "not compatible")
}
