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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	internal2 "github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func constructTestTablePrimitiveTypes(t *testing.T) (*metadata.FileMetaData, table.Metadata) {
	tableMeta, err := table.ParseMetadataString(`{
        "format-version": 2,
        "location": "s3://bucket/test/location",
        "last-column-id": 7,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "booleans", "required": false, "type": "boolean"},
                    {"id": 2, "name": "ints", "required": false, "type": "int"},
                    {"id": 3, "name": "longs", "required": false, "type": "long"},
                    {"id": 4, "name": "floats", "required": false, "type": "float"},
                    {"id": 5, "name": "doubles", "required": false, "type": "double"},
                    {"id": 6, "name": "dates", "required": false, "type": "date"},
                    {"id": 7, "name": "times", "required": false, "type": "time"},
                    {"id": 8, "name": "timestamps", "required": false, "type": "timestamp"},
                    {"id": 9, "name": "timestamptzs", "required": false, "type": "timestamptz"},
                    {"id": 10, "name": "strings", "required": false, "type": "string"},
                    {"id": 11, "name": "uuids", "required": false, "type": "uuid"},
                    {"id": 12, "name": "binaries", "required": false, "type": "binary"},
					{"id": 13, "name": "small_dec", "required": false, "type": "decimal(8, 2)"},
					{"id": 14, "name": "med_dec", "required": false, "type": "decimal(16, 2)"},
					{"id": 15, "name": "large_dec", "required": false, "type": "decimal(24, 2)"}
                ]
            }
        ],
		"last-partition-id": 0,	
		"last-updated-ms": -1,
        "default-spec-id": 0,
		"default-sort-order-id": 0,
		"sort-orders": [{"order-id": 0, "fields": []}],
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "properties": {}
	}`)
	require.NoError(t, err)

	arrowSchema, err := table.SchemaToArrowSchema(tableMeta.Schemas()[0], nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[
		{
			"booleans": true, 
			"ints": 23,
			"longs": 54,
			"floats": 454.1223,
			"doubles": 8542.12,
			"dates": "2022-01-02",
			"times": "17:30:34",
			"timestamps": "2022-01-02T17:30:34.399",
			"timestamptzs": "2022-01-02T17:30:34.399",
			"strings": "hello",
			"uuids": "`+uuid.NewMD5(uuid.NameSpaceDNS, []byte("foo")).String()+`",
			"binaries": "aGVsbG8=",
			"small_dec": "123456.78",
			"med_dec": "12345678901234.56",
			"large_dec": "1234567890123456789012.34"
		},
		{
			"booleans": false,
			"ints": 89,
			"longs": 2,
			"floats": 24342.29,
			"doubles": -43.9,
			"dates": "2023-02-04",
			"times": "13:21:04",
			"timestamps": "2023-02-04T13:21:04.354",
			"timestamptzs": "2023-02-04T13:21:04.354",
			"strings": "world",
			"uuids": "`+uuid.NewMD5(uuid.NameSpaceDNS, []byte("bar")).String()+`",
			"binaries": "d29ybGQ=",
			"small_dec": "876543.21",
			"med_dec": "65432109876543.21",
			"large_dec": "4321098765432109876543.21"
		}
	]`))
	require.NoError(t, err)
	defer rec.Release()

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(arrowSchema, &buf,
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	return rdr.MetaData(), tableMeta
}

func assertBounds[T iceberg.LiteralType](t *testing.T, bound []byte, typ iceberg.Type, expected T) {
	lit, err := iceberg.LiteralFromBytes(typ, bound)
	require.NoError(t, err)
	assert.Equal(t, expected, lit.(iceberg.TypedLiteral[T]).Value())
}

func toDate(tm time.Time) iceberg.Date {
	return iceberg.Date(tm.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds()))
}

func getCollector() map[int]internal.StatisticsCollector {
	modeTrunc := internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 2}
	modeFull := internal.MetricsMode{Typ: internal.MetricModeFull}

	return map[int]internal.StatisticsCollector{
		1: {
			FieldID:    1,
			Mode:       modeFull,
			ColName:    "booleans",
			IcebergTyp: iceberg.PrimitiveTypes.Bool,
		},
		2: {
			FieldID:    2,
			Mode:       modeFull,
			ColName:    "ints",
			IcebergTyp: iceberg.PrimitiveTypes.Int32,
		},
		3: {
			FieldID:    3,
			Mode:       modeFull,
			ColName:    "longs",
			IcebergTyp: iceberg.PrimitiveTypes.Int64,
		},
		4: {
			FieldID:    4,
			Mode:       modeFull,
			ColName:    "floats",
			IcebergTyp: iceberg.PrimitiveTypes.Float32,
		},
		5: {
			FieldID:    5,
			Mode:       modeFull,
			ColName:    "doubles",
			IcebergTyp: iceberg.PrimitiveTypes.Float64,
		},
		6: {
			FieldID:    6,
			Mode:       modeFull,
			ColName:    "dates",
			IcebergTyp: iceberg.PrimitiveTypes.Date,
		},
		7: {
			FieldID:    7,
			Mode:       modeFull,
			ColName:    "times",
			IcebergTyp: iceberg.PrimitiveTypes.Time,
		},
		8: {
			FieldID:    8,
			Mode:       modeFull,
			ColName:    "timestamps",
			IcebergTyp: iceberg.PrimitiveTypes.Timestamp,
		},
		9: {
			FieldID:    9,
			Mode:       modeFull,
			ColName:    "timestamptzs",
			IcebergTyp: iceberg.PrimitiveTypes.TimestampTz,
		},
		10: {
			FieldID:    10,
			Mode:       modeTrunc,
			ColName:    "strings",
			IcebergTyp: iceberg.PrimitiveTypes.String,
		},
		11: {
			FieldID:    11,
			Mode:       modeFull,
			ColName:    "uuids",
			IcebergTyp: iceberg.PrimitiveTypes.UUID,
		},
		12: {
			FieldID:    12,
			Mode:       modeTrunc,
			ColName:    "binaries",
			IcebergTyp: iceberg.PrimitiveTypes.Binary,
		},
		13: {
			FieldID:    13,
			Mode:       modeFull,
			ColName:    "small_dec",
			IcebergTyp: iceberg.DecimalTypeOf(8, 2),
		},
		14: {
			FieldID:    14,
			Mode:       modeFull,
			ColName:    "med_dec",
			IcebergTyp: iceberg.DecimalTypeOf(16, 2),
		},
		15: {
			FieldID:    15,
			Mode:       modeFull,
			ColName:    "large_dec",
			IcebergTyp: iceberg.DecimalTypeOf(24, 2),
		},
	}
}

func TestMetricsPrimitiveTypes(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	meta, tblMeta := constructTestTablePrimitiveTypes(t)
	require.NotNil(t, tblMeta)
	require.NotNil(t, meta)

	mapping, err := format.PathToIDMapping(tblMeta.CurrentSchema())
	require.NoError(t, err)

	stats := format.DataFileStatsFromMeta(internal.Metadata(meta), getCollector(), mapping, nil)
	const sortOrderID = 7
	df := stats.ToDataFile(internal.DataFileOpts{
		Schema:      tblMeta.CurrentSchema(),
		Spec:        tblMeta.PartitionSpec(),
		Path:        "fake-path.parquet",
		Format:      iceberg.ParquetFile,
		Content:     iceberg.EntryContentData,
		FileSize:    meta.GetSourceFileSize(),
		SortOrderID: sortOrderID,
	})

	require.NotNil(t, df.SortOrderID())
	assert.Equal(t, sortOrderID, *df.SortOrderID())

	assert.Len(t, df.ValueCounts(), 15)
	assert.Len(t, df.NullValueCounts(), 15)
	assert.Len(t, df.NaNValueCounts(), 0)

	assert.Len(t, df.LowerBoundValues(), 15)
	assertBounds(t, df.LowerBoundValues()[1], iceberg.PrimitiveTypes.Bool, false)
	assertBounds(t, df.LowerBoundValues()[2], iceberg.PrimitiveTypes.Int32, int32(23))
	assertBounds(t, df.LowerBoundValues()[3], iceberg.PrimitiveTypes.Int64, int64(2))
	assertBounds(t, df.LowerBoundValues()[4], iceberg.PrimitiveTypes.Float32, float32(454.1223))
	assertBounds(t, df.LowerBoundValues()[5], iceberg.PrimitiveTypes.Float64, -43.9)
	assertBounds(t, df.LowerBoundValues()[6], iceberg.PrimitiveTypes.Date,
		toDate(time.Date(2022, time.January, 2, 0, 0, 0, 0, time.UTC)))
	assertBounds(t, df.LowerBoundValues()[7], iceberg.PrimitiveTypes.Time,
		iceberg.Time(time.Duration(13*time.Hour+21*time.Minute+4*time.Second).Microseconds()))
	assertBounds(t, df.LowerBoundValues()[8], iceberg.PrimitiveTypes.Timestamp,
		iceberg.Timestamp(time.Date(2022, time.January, 2, 17, 30, 34, 399000000, time.UTC).UnixMicro()))
	assertBounds(t, df.LowerBoundValues()[9], iceberg.PrimitiveTypes.TimestampTz,
		iceberg.Timestamp(time.Date(2022, time.January, 2, 17, 30, 34, 399000000, time.UTC).UnixMicro()))
	assertBounds(t, df.LowerBoundValues()[10], iceberg.PrimitiveTypes.String, "he")
	assertBounds(t, df.LowerBoundValues()[11], iceberg.PrimitiveTypes.UUID,
		uuid.NewMD5(uuid.NameSpaceDNS, []byte("foo")))
	assertBounds(t, df.LowerBoundValues()[12], iceberg.PrimitiveTypes.Binary, []byte("he"))
	assertBounds(t, df.LowerBoundValues()[13], iceberg.DecimalTypeOf(8, 2), iceberg.Decimal{
		Val:   decimal128.FromI64(12345678),
		Scale: 2,
	})
	assertBounds(t, df.LowerBoundValues()[14], iceberg.DecimalTypeOf(16, 2), iceberg.Decimal{
		Val:   decimal128.FromI64(1234567890123456),
		Scale: 2,
	})
	expected, _ := (&big.Int{}).SetString("123456789012345678901234", 10)
	assertBounds(t, df.LowerBoundValues()[15], iceberg.DecimalTypeOf(24, 2), iceberg.Decimal{
		Val:   decimal128.FromBigInt(expected),
		Scale: 2,
	})

	assert.Len(t, df.UpperBoundValues(), 15)
	assertBounds(t, df.UpperBoundValues()[1], iceberg.PrimitiveTypes.Bool, true)
	assertBounds(t, df.UpperBoundValues()[2], iceberg.PrimitiveTypes.Int32, int32(89))
	assertBounds(t, df.UpperBoundValues()[3], iceberg.PrimitiveTypes.Int64, int64(54))
	assertBounds(t, df.UpperBoundValues()[4], iceberg.PrimitiveTypes.Float32, float32(24342.29))
	assertBounds(t, df.UpperBoundValues()[5], iceberg.PrimitiveTypes.Float64, 8542.12)
	assertBounds(t, df.UpperBoundValues()[6], iceberg.PrimitiveTypes.Date,
		toDate(time.Date(2023, time.February, 4, 0, 0, 0, 0, time.UTC)))
	assertBounds(t, df.UpperBoundValues()[7], iceberg.PrimitiveTypes.Time,
		iceberg.Time(time.Duration(17*time.Hour+30*time.Minute+34*time.Second).Microseconds()))
	assertBounds(t, df.UpperBoundValues()[8], iceberg.PrimitiveTypes.Timestamp,
		iceberg.Timestamp(time.Date(2023, time.February, 4, 13, 21, 4, 354000000, time.UTC).UnixMicro()))
	assertBounds(t, df.UpperBoundValues()[9], iceberg.PrimitiveTypes.TimestampTz,
		iceberg.Timestamp(time.Date(2023, time.February, 4, 13, 21, 4, 354000000, time.UTC).UnixMicro()))
	assertBounds(t, df.UpperBoundValues()[10], iceberg.PrimitiveTypes.String, "wp")
	assertBounds(t, df.UpperBoundValues()[11], iceberg.PrimitiveTypes.UUID,
		uuid.NewMD5(uuid.NameSpaceDNS, []byte("bar")))
	assertBounds(t, df.UpperBoundValues()[12], iceberg.PrimitiveTypes.Binary, []byte("wp"))
	assertBounds(t, df.UpperBoundValues()[13], iceberg.DecimalTypeOf(8, 2), iceberg.Decimal{
		Val:   decimal128.FromI64(87654321),
		Scale: 2,
	})
	assertBounds(t, df.UpperBoundValues()[14], iceberg.DecimalTypeOf(16, 2), iceberg.Decimal{
		Val:   decimal128.FromI64(6543210987654321),
		Scale: 2,
	})
	expectedUpper, _ := (&big.Int{}).SetString("432109876543210987654321", 10)
	assertBounds(t, df.UpperBoundValues()[15], iceberg.DecimalTypeOf(24, 2), iceberg.Decimal{
		Val:   decimal128.FromBigInt(expectedUpper),
		Scale: 2,
	})
}

// TestNanosecondTimestampMetrics tests that nanosecond timestamp types (v3)
// are correctly handled for Parquet stats collection and physical type mapping.
func TestNanosecondTimestampMetrics(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	tableMeta, err := table.ParseMetadataString(`{
		"format-version": 3,
		"location": "s3://bucket/test/location",
		"last-column-id": 2,
		"current-schema-id": 0,
		"next-row-id": 0,
		"schemas": [
			{
				"type": "struct",
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "ts_ns", "required": false, "type": "timestamp_ns"},
					{"id": 2, "name": "tstz_ns", "required": false, "type": "timestamptz_ns"}
				]
			}
		],
		"last-partition-id": 0,
		"last-updated-ms": -1,
		"default-spec-id": 0,
		"default-sort-order-id": 0,
		"sort-orders": [{"order-id": 0, "fields": []}],
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"properties": {}
	}`)
	require.NoError(t, err)

	arrowSchema, err := table.SchemaToArrowSchema(tableMeta.Schemas()[0], nil, true, false)
	require.NoError(t, err)

	// Build records manually using Arrow builders since JSON parsing for
	// nanosecond timestamps needs explicit int64 values (nanoseconds since epoch).
	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, arrowSchema)
	defer bldr.Release()

	// 2022-01-02T17:30:34.399000000 in nanoseconds since epoch
	ts1 := time.Date(2022, time.January, 2, 17, 30, 34, 399000000, time.UTC).UnixNano()
	// 2023-02-04T13:21:04.354000000 in nanoseconds since epoch
	ts2 := time.Date(2023, time.February, 4, 13, 21, 4, 354000000, time.UTC).UnixNano()

	bldr.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts1))
	bldr.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(ts1))

	bldr.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts2))
	bldr.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(ts2))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(arrowSchema, &buf,
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	meta := rdr.MetaData()

	mapping, err := format.PathToIDMapping(tableMeta.CurrentSchema())
	require.NoError(t, err)

	modeFull := internal.MetricsMode{Typ: internal.MetricModeFull}
	collector := map[int]internal.StatisticsCollector{
		1: {FieldID: 1, Mode: modeFull, ColName: "ts_ns", IcebergTyp: iceberg.PrimitiveTypes.TimestampNs},
		2: {FieldID: 2, Mode: modeFull, ColName: "tstz_ns", IcebergTyp: iceberg.PrimitiveTypes.TimestampTzNs},
	}

	stats := format.DataFileStatsFromMeta(internal.Metadata(meta), collector, mapping, nil)
	df := stats.ToDataFile(internal.DataFileOpts{
		Schema:   tableMeta.CurrentSchema(),
		Spec:     tableMeta.PartitionSpec(),
		Path:     "fake-path.parquet",
		Format:   iceberg.ParquetFile,
		Content:  iceberg.EntryContentData,
		FileSize: meta.GetSourceFileSize(),
	})

	assert.Len(t, df.ValueCounts(), 2)
	assert.Len(t, df.NullValueCounts(), 2)

	assert.Len(t, df.LowerBoundValues(), 2)
	assertBounds(t, df.LowerBoundValues()[1], iceberg.PrimitiveTypes.TimestampNs, iceberg.TimestampNano(ts1))
	assertBounds(t, df.LowerBoundValues()[2], iceberg.PrimitiveTypes.TimestampTzNs, iceberg.TimestampNano(ts1))

	assert.Len(t, df.UpperBoundValues(), 2)
	assertBounds(t, df.UpperBoundValues()[1], iceberg.PrimitiveTypes.TimestampNs, iceberg.TimestampNano(ts2))
	assertBounds(t, df.UpperBoundValues()[2], iceberg.PrimitiveTypes.TimestampTzNs, iceberg.TimestampNano(ts2))
}

// TestDecimalPhysicalTypes tests that decimals stored as INT32/INT64 physical types
// are correctly handled. This is important because Parquet allows decimals with
// precision <= 9 to be stored as INT32, and precision <= 18 as INT64.
func TestDecimalPhysicalTypes(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	tests := []struct {
		name         string
		precision    int
		scale        int
		physicalType parquet.Type
		values       []int64 // unscaled values
		expectedMin  int64
		expectedMax  int64
	}{
		{
			name:         "decimal_as_int32",
			precision:    7,
			scale:        2,
			physicalType: parquet.Types.Int32,
			values:       []int64{12345, 67890}, // represents 123.45, 678.90
			expectedMin:  12345,
			expectedMax:  67890,
		},
		{
			name:         "decimal_as_int64",
			precision:    15,
			scale:        2,
			physicalType: parquet.Types.Int64,
			values:       []int64{123456789012345, 987654321098765},
			expectedMin:  123456789012345,
			expectedMax:  987654321098765,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a Parquet file with decimal stored as INT32 or INT64
			var buf bytes.Buffer

			// Build a custom schema with decimal logical type
			decType := schema.NewDecimalLogicalType(int32(tt.precision), int32(tt.scale))
			var node schema.Node
			var err error
			if tt.physicalType == parquet.Types.Int32 {
				node, err = schema.NewPrimitiveNodeLogical("value", parquet.Repetitions.Required,
					decType, parquet.Types.Int32, 0, 1)
			} else {
				node, err = schema.NewPrimitiveNodeLogical("value", parquet.Repetitions.Required,
					decType, parquet.Types.Int64, 0, 1)
			}
			require.NoError(t, err)

			rootNode, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{node}, -1)
			require.NoError(t, err)

			// Write the parquet file
			writer := file.NewParquetWriter(&buf,
				rootNode,
				file.WithWriterProps(parquet.NewWriterProperties(parquet.WithStats(true))))

			rgw := writer.AppendRowGroup()
			colWriter, err := rgw.NextColumn()
			require.NoError(t, err)

			if tt.physicalType == parquet.Types.Int32 {
				int32Writer := colWriter.(*file.Int32ColumnChunkWriter)
				vals := make([]int32, len(tt.values))
				for i, v := range tt.values {
					vals[i] = int32(v)
				}
				_, err = int32Writer.WriteBatch(vals, nil, nil)
			} else {
				int64Writer := colWriter.(*file.Int64ColumnChunkWriter)
				_, err = int64Writer.WriteBatch(tt.values, nil, nil)
			}
			require.NoError(t, err)

			require.NoError(t, colWriter.Close())
			require.NoError(t, rgw.Close())
			require.NoError(t, writer.Close())

			// Read back and get metadata
			rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			defer rdr.Close()

			meta := rdr.MetaData()

			// Create table metadata with decimal type
			tableMeta, err := table.ParseMetadataString(fmt.Sprintf(`{
				"format-version": 2,
				"location": "s3://bucket/test/location",
				"last-column-id": 1,
				"current-schema-id": 0,
				"schemas": [
					{
						"type": "struct",
						"schema-id": 0,
						"fields": [
							{"id": 1, "name": "value", "required": true, "type": "decimal(%d, %d)"}
						]
					}
				],
				"last-partition-id": 0,
				"last-updated-ms": -1,
				"default-spec-id": 0,
				"default-sort-order-id": 0,
				"sort-orders": [{"order-id": 0, "fields": []}],
				"partition-specs": [{"spec-id": 0, "fields": []}],
				"properties": {}
			}`, tt.precision, tt.scale))
			require.NoError(t, err)

			mapping, err := format.PathToIDMapping(tableMeta.CurrentSchema())
			require.NoError(t, err)

			collector := map[int]internal.StatisticsCollector{
				1: {
					FieldID:    1,
					Mode:       internal.MetricsMode{Typ: internal.MetricModeFull},
					ColName:    "value",
					IcebergTyp: iceberg.DecimalTypeOf(tt.precision, tt.scale),
				},
			}

			// This should not panic - the fix allows INT32/INT64 physical types for decimals
			stats := format.DataFileStatsFromMeta(internal.Metadata(meta), collector, mapping, nil)
			require.NotNil(t, stats)

			df := stats.ToDataFile(internal.DataFileOpts{
				Schema:   tableMeta.CurrentSchema(),
				Spec:     tableMeta.PartitionSpec(),
				Path:     "test.parquet",
				Format:   iceberg.ParquetFile,
				Content:  iceberg.EntryContentData,
				FileSize: meta.GetSourceFileSize(),
			})

			// Verify bounds are correctly extracted
			require.Contains(t, df.LowerBoundValues(), 1)
			require.Contains(t, df.UpperBoundValues(), 1)

			// Verify the actual values
			minLit, err := iceberg.LiteralFromBytes(iceberg.DecimalTypeOf(tt.precision, tt.scale), df.LowerBoundValues()[1])
			require.NoError(t, err)
			minDec := minLit.(iceberg.TypedLiteral[iceberg.Decimal]).Value()
			assert.Equal(t, uint64(tt.expectedMin), minDec.Val.LowBits())
			assert.Equal(t, tt.scale, minDec.Scale)

			maxLit, err := iceberg.LiteralFromBytes(iceberg.DecimalTypeOf(tt.precision, tt.scale), df.UpperBoundValues()[1])
			require.NoError(t, err)
			maxDec := maxLit.(iceberg.TypedLiteral[iceberg.Decimal]).Value()
			assert.Equal(t, uint64(tt.expectedMax), maxDec.Val.LowBits())
			assert.Equal(t, tt.scale, maxDec.Scale)
		})
	}
}

func TestWriteDataFileErrOnClose(t *testing.T) {
	ctx := context.Background()
	fm := internal.GetFileFormat(iceberg.ParquetFile)
	mockfs := internal2.MockFS{}
	mockfs.Test(t)

	mockfs.On("Create", "f").Return(&internal2.MockFile{
		ErrOnClose: true,
	}, nil)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "nested",
			Type: arrow.ListOfField(arrow.Field{
				Name: "element", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
				Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"2"}),
			}),
			Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"1"}),
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	bldr.Field(0).AppendNull()
	defer bldr.Release()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	icesc, err := table.ArrowSchemaToIceberg(schema, false, nil)
	require.NoError(t, err)

	_, err = fm.WriteDataFile(ctx, &mockfs, nil, internal.WriteFileInfo{
		FileSchema: icesc,
		Spec:       iceberg.PartitionSpec{},
		FileName:   "f",
		StatsCols:  nil,
		WriteProps: []parquet.WriterProperty{},
	}, []arrow.RecordBatch{rec})
	require.ErrorContains(t, err, "error on close")
}

func TestGetWritePropertiesPageVersion(t *testing.T) {
	tests := []struct {
		name             string
		props            iceberg.Properties
		expectedPageType file.PageType
	}{
		{
			name:             "default is v2",
			props:            iceberg.Properties{},
			expectedPageType: file.PageTypeDataPageV2,
		},
		{
			name:             "explicit v2",
			props:            iceberg.Properties{internal.ParquetPageVersionKey: "2"},
			expectedPageType: file.PageTypeDataPageV2,
		},
		{
			name:             "explicit v1",
			props:            iceberg.Properties{internal.ParquetPageVersionKey: "1"},
			expectedPageType: file.PageTypeDataPage,
		},
		{
			name:             "invalid falls back to v2",
			props:            iceberg.Properties{internal.ParquetPageVersionKey: "invalid"},
			expectedPageType: file.PageTypeDataPageV2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			format := internal.GetFileFormat(iceberg.ParquetFile)
			writeProps := format.GetWriteProperties(tt.props).([]parquet.WriterProperty)

			root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
				schema.NewInt32Node("col", parquet.Repetitions.Required, -1),
			}, -1)
			require.NoError(t, err)

			var buf bytes.Buffer
			pw := file.NewParquetWriter(&buf, root, file.WithWriterProps(
				parquet.NewWriterProperties(writeProps...),
			))

			rgw := pw.AppendRowGroup()
			cw, _ := rgw.NextColumn()
			cw.(*file.Int32ColumnChunkWriter).WriteBatch(
				[]int32{1, 2, 3}, nil, nil,
			)
			cw.Close()
			rgw.Close()
			pw.Close()

			rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			defer rdr.Close()

			pageRdr, err := rdr.RowGroup(0).GetColumnPageReader(0)
			require.NoError(t, err)

			require.True(t, pageRdr.Next())
			assert.Equal(t, tt.expectedPageType, pageRdr.Page().Type())
		})
	}
}

func TestGetWritePropertiesBloomFilter(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	t.Run("default max bloom filter bytes", func(t *testing.T) {
		wp := parquet.NewWriterProperties(format.GetWriteProperties(iceberg.Properties{}).([]parquet.WriterProperty)...)
		assert.Equal(t, int64(internal.ParquetBloomFilterMaxBytesDefault), wp.MaxBloomFilterBytes())
	})

	t.Run("custom max bloom filter bytes", func(t *testing.T) {
		props := iceberg.Properties{
			internal.ParquetBloomFilterMaxBytesKey: "2097152",
		}
		wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
		assert.Equal(t, int64(2097152), wp.MaxBloomFilterBytes())
	})

	t.Run("adaptive bloom filter and candidates", func(t *testing.T) {
		wp := parquet.NewWriterProperties(format.GetWriteProperties(iceberg.Properties{}).([]parquet.WriterProperty)...)
		assert.False(t, wp.AdaptiveBloomFilterEnabledFor("id"), "adaptive stays off by default")
		assert.Equal(t, parquet.DefaultBloomFilterCandidates, wp.BloomFilterCandidatesFor("id"))

		props := iceberg.Properties{
			internal.ParquetBloomFilterAdaptiveEnabledKey: "true",
			internal.ParquetBloomFilterCandidatesKey:      "16",
		}
		wp = parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
		assert.True(t, wp.AdaptiveBloomFilterEnabledFor("id"))
		assert.Equal(t, 16, wp.BloomFilterCandidatesFor("id"))
	})

	t.Run("per-column bloom filter enabled", func(t *testing.T) {
		props := iceberg.Properties{
			internal.ParquetBloomFilterColumnEnabledKeyPrefix + ".id":   "true",
			internal.ParquetBloomFilterColumnEnabledKeyPrefix + ".name": "false",
		}
		wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
		assert.True(t, wp.BloomFilterEnabledFor("id"))
		assert.False(t, wp.BloomFilterEnabledFor("name"))
		assert.False(t, wp.BloomFilterEnabledFor("unmentioned_col"), "columns absent from properties must default to no bloom filter")
	})
}

// REDEYE PATCH test: write.parquet.dictionary-enabled controls Parquet dictionary
// encoding. Default (absent property) stays OFF to match upstream; "true" turns it ON.
func TestGetWritePropertiesDictionary(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	t.Run("dictionary disabled by default (upstream behavior)", func(t *testing.T) {
		wp := parquet.NewWriterProperties(format.GetWriteProperties(iceberg.Properties{}).([]parquet.WriterProperty)...)
		assert.False(t, wp.DictionaryEnabled(), "absent property must keep dictionary OFF (upstream default)")
	})

	t.Run("dictionary enabled when property is true", func(t *testing.T) {
		props := iceberg.Properties{internal.ParquetDictEnabledKey: "true"}
		wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
		assert.True(t, wp.DictionaryEnabled(), "write.parquet.dictionary-enabled=true must turn dictionary ON")
	})

	t.Run("dictionary disabled when property is false", func(t *testing.T) {
		props := iceberg.Properties{internal.ParquetDictEnabledKey: "false"}
		wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
		assert.False(t, wp.DictionaryEnabled(), "write.parquet.dictionary-enabled=false must keep dictionary OFF")
	})

	// REDEYE PATCH (per-column): write.parquet.dictionary-enabled.column.<col> resolves
	// per-column, mirroring the bloom-filter per-column prefix, and takes precedence over
	// the global default for the named column. This is the property-resolution level.
	t.Run("per-column dictionary resolves per column", func(t *testing.T) {
		props := iceberg.Properties{
			internal.ParquetDictEnabledColumnKeyPrefix + ".index":      "true",
			internal.ParquetDictEnabledColumnKeyPrefix + ".host":       "true",
			internal.ParquetDictEnabledColumnKeyPrefix + ".source":     "true",
			internal.ParquetDictEnabledColumnKeyPrefix + ".sourcetype": "true",
		}
		wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
		assert.False(t, wp.DictionaryEnabled(), "global default must stay OFF when only per-column keys are set")
		assert.True(t, wp.DictionaryEnabledFor("index"), "index must be dictionary-enabled")
		assert.True(t, wp.DictionaryEnabledFor("host"), "host must be dictionary-enabled")
		assert.True(t, wp.DictionaryEnabledFor("source"), "source must be dictionary-enabled")
		assert.True(t, wp.DictionaryEnabledFor("sourcetype"), "sourcetype must be dictionary-enabled")
		assert.False(t, wp.DictionaryEnabledFor("_time"), "unlisted _time must stay PLAIN (global default)")
		assert.False(t, wp.DictionaryEnabledFor("_raw"), "unlisted _raw must stay PLAIN (global default)")
	})

	t.Run("per-column key overrides true global default to false", func(t *testing.T) {
		props := iceberg.Properties{
			internal.ParquetDictEnabledKey:                       "true",
			internal.ParquetDictEnabledColumnKeyPrefix + "._raw": "false",
		}
		wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
		assert.True(t, wp.DictionaryEnabled(), "global default ON")
		assert.True(t, wp.DictionaryEnabledFor("index"), "unlisted column follows ON global default")
		assert.False(t, wp.DictionaryEnabledFor("_raw"), "per-column false must override the ON global default")
	})

	// Encoding-level proof: a per-column dict prop must make that column come out
	// RLE_DICTIONARY in the actual parquet, while an unlisted column stays PLAIN.
	t.Run("per-column dictionary yields RLE_DICTIONARY for named column, PLAIN for unlisted", func(t *testing.T) {
		props := iceberg.Properties{
			internal.ParquetDictEnabledColumnKeyPrefix + ".index": "true",
			// "_raw" intentionally unlisted -> must stay PLAIN.
		}
		writeProps := format.GetWriteProperties(props).([]parquet.WriterProperty)

		root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
			schema.NewByteArrayNode("index", parquet.Repetitions.Required, -1),
			schema.NewByteArrayNode("_raw", parquet.Repetitions.Required, -1),
		}, -1)
		require.NoError(t, err)

		var buf bytes.Buffer
		pw := file.NewParquetWriter(&buf, root, file.WithWriterProps(
			parquet.NewWriterProperties(writeProps...),
		))
		rgw := pw.AppendRowGroup()

		// index: highly repeated low-cardinality value (dictionary should kick in).
		idxVals := make([]parquet.ByteArray, 200)
		for i := range idxVals {
			idxVals[i] = parquet.ByteArray("main")
		}
		cwIdx, _ := rgw.NextColumn()
		_, err = cwIdx.(*file.ByteArrayColumnChunkWriter).WriteBatch(idxVals, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cwIdx.Close())

		// _raw: also repeated, but unlisted -> must stay PLAIN despite low cardinality.
		rawVals := make([]parquet.ByteArray, 200)
		for i := range rawVals {
			rawVals[i] = parquet.ByteArray("raw-line")
		}
		cwRaw, _ := rgw.NextColumn()
		_, err = cwRaw.(*file.ByteArrayColumnChunkWriter).WriteBatch(rawVals, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cwRaw.Close())

		require.NoError(t, rgw.Close())
		require.NoError(t, pw.Close())

		rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		defer rdr.Close()

		hasDict := func(encs []parquet.Encoding) bool {
			for _, e := range encs {
				if e == parquet.Encodings.RLEDict || e == parquet.Encodings.PlainDict {
					return true
				}
			}
			return false
		}

		rg := rdr.MetaData().RowGroup(0)
		idxChunk, err := rg.ColumnChunk(0)
		require.NoError(t, err)
		rawChunk, err := rg.ColumnChunk(1)
		require.NoError(t, err)

		assert.True(t, hasDict(idxChunk.Encodings()),
			"index (per-column dict enabled) must be dictionary-encoded, got %v", idxChunk.Encodings())
		assert.False(t, hasDict(rawChunk.Encodings()),
			"_raw (unlisted) must stay PLAIN, got %v", rawChunk.Encodings())
	})
}

// REDEYE PATCH test: write.parquet.encoding.column.<col-name>.
//
// The assertion that matters is the last one, and it is on the WRITTEN FILE rather than on
// the property list: a property that is accepted and then does not change the bytes is the
// failure this patch exists to avoid. caver-go #3032 was about to add an encoding table
// property to a writer that had no encoding hook at all, redeploy, measure no change, and
// conclude delta encoding does not help.
func TestGetWritePropertiesEncodingColumn(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	t.Run("known names map, unknown names are ignored not fatal", func(t *testing.T) {
		for _, name := range []string{"DELTA_BINARY_PACKED", "delta_binary_packed", " Delta_Binary_Packed "} {
			props := iceberg.Properties{internal.ParquetEncodingColumnKeyPrefix + "._time": name}
			assert.NotPanics(t, func() { format.GetWriteProperties(props) }, "name %q", name)
		}
		// An operator typo must not take the writer down; the column keeps its default.
		props := iceberg.Properties{internal.ParquetEncodingColumnKeyPrefix + "._time": "DLETA_BINARY_PACKED"}
		assert.NotPanics(t, func() { format.GetWriteProperties(props) })
	})

	t.Run("an int64 column comes out DELTA_BINARY_PACKED, an unlisted one stays PLAIN", func(t *testing.T) {
		props := iceberg.Properties{
			internal.ParquetEncodingColumnKeyPrefix + "._time": "DELTA_BINARY_PACKED",
			// "other" intentionally unlisted -> must stay PLAIN.
		}
		writeProps := format.GetWriteProperties(props).([]parquet.WriterProperty)

		root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
			schema.NewInt64Node("_time", parquet.Repetitions.Required, -1),
			schema.NewInt64Node("other", parquet.Repetitions.Required, -1),
		}, -1)
		require.NoError(t, err)

		var buf bytes.Buffer
		pw := file.NewParquetWriter(&buf, root, file.WithWriterProps(
			parquet.NewWriterProperties(writeProps...),
		))
		rgw := pw.AppendRowGroup()

		// Microsecond timestamps a millisecond apart: what the lake actually writes.
		vals := make([]int64, 500)
		for i := range vals {
			vals[i] = int64(1_756_000_000_000_000 + i*1000)
		}
		cwTime, _ := rgw.NextColumn()
		_, err = cwTime.(*file.Int64ColumnChunkWriter).WriteBatch(vals, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cwTime.Close())

		cwOther, _ := rgw.NextColumn()
		_, err = cwOther.(*file.Int64ColumnChunkWriter).WriteBatch(vals, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cwOther.Close())

		require.NoError(t, rgw.Close())
		require.NoError(t, pw.Close())

		rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		defer rdr.Close()

		has := func(encs []parquet.Encoding, want parquet.Encoding) bool {
			for _, e := range encs {
				if e == want {
					return true
				}
			}
			return false
		}

		rg := rdr.MetaData().RowGroup(0)
		timeChunk, err := rg.ColumnChunk(0)
		require.NoError(t, err)
		otherChunk, err := rg.ColumnChunk(1)
		require.NoError(t, err)

		assert.True(t, has(timeChunk.Encodings(), parquet.Encodings.DeltaBinaryPacked),
			"_time (per-column encoding set) must be DELTA_BINARY_PACKED, got %v", timeChunk.Encodings())
		assert.False(t, has(otherChunk.Encodings(), parquet.Encodings.DeltaBinaryPacked),
			"other (unlisted) must not be delta-encoded, got %v", otherChunk.Encodings())

		// The point of the whole exercise: delta must actually be SMALLER on this data.
		// Without this, the patch could ship an encoding that is accepted, applied, and a
		// pessimisation.
		t.Logf("MEASURED: delta _time=%d bytes, PLAIN other=%d bytes (%.1fx smaller)",
			timeChunk.TotalCompressedSize(), otherChunk.TotalCompressedSize(),
			float64(otherChunk.TotalCompressedSize())/float64(timeChunk.TotalCompressedSize()))
		assert.Less(t, timeChunk.TotalCompressedSize(), otherChunk.TotalCompressedSize(),
			"delta-encoded _time (%d bytes) must be smaller than the PLAIN column (%d bytes)",
			timeChunk.TotalCompressedSize(), otherChunk.TotalCompressedSize())
	})
}

// The honest number for the data caver-go actually writes.
//
// The test above uses perfectly ordered timestamps and gets ~10x, which is the BEST case
// and not our case: the lake sorts on the host dimension (CAVER_LAKE_COMPACT_SORT=1), so
// _time arrives at the encoder interleaved across hosts. caver-go #3032 is about exactly
// that. Quoting the ordered number as the expected win would be the same error as quoting
// a 15% estate saving from an unmeasured premise, which is what #3032's own author had to
// withdraw.
//
// So this measures the interleaved shape and asserts only what it can: delta must still be
// a WIN, not a pessimisation. Whatever the ratio turns out to be is logged rather than
// asserted, because the number belongs to the data, not to the patch.
func TestEncodingColumnOnInterleavedTimestamps(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	write := func(t *testing.T, encoded bool, vals []int64) int64 {
		t.Helper()
		// zstd on both sides, stated explicitly rather than relied on: it is already
		// ParquetCompressionDefault, so setting it changes nothing here and is written down
		// only so a reader knows what the ratio below MEANS. It is what delta adds ON TOP
		// of the codec, not delta versus raw PLAIN. Getting that wrong would report a
		// number the estate can never reach.
		props := iceberg.Properties{internal.ParquetCompressionKey: "zstd"}
		if encoded {
			props[internal.ParquetEncodingColumnKeyPrefix+"._time"] = "DELTA_BINARY_PACKED"
		}
		writeProps := format.GetWriteProperties(props).([]parquet.WriterProperty)
		root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
			schema.NewInt64Node("_time", parquet.Repetitions.Required, -1),
		}, -1)
		require.NoError(t, err)
		var buf bytes.Buffer
		pw := file.NewParquetWriter(&buf, root, file.WithWriterProps(parquet.NewWriterProperties(writeProps...)))
		rgw := pw.AppendRowGroup()
		cw, _ := rgw.NextColumn()
		_, err = cw.(*file.Int64ColumnChunkWriter).WriteBatch(vals, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cw.Close())
		require.NoError(t, rgw.Close())
		require.NoError(t, pw.Close())
		rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		defer rdr.Close()
		ch, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
		require.NoError(t, err)
		return ch.TotalCompressedSize()
	}

	// 40 hosts, each with its own contiguous run, which is what a host-sorted file looks
	// like: monotonic WITHIN a run, jumping backwards at every run boundary.
	const hosts, perHost = 40, 50
	interleaved := make([]int64, 0, hosts*perHost)
	base := int64(1_756_000_000_000_000)
	for h := 0; h < hosts; h++ {
		for i := 0; i < perHost; i++ {
			interleaved = append(interleaved, base+int64(i)*1000+int64(h)*7)
		}
	}

	plain := write(t, false, interleaved)
	delta := write(t, true, interleaved)

	// The same measurement on a perfectly ORDERED column, so the gap between the two is
	// visible rather than assumed. This is the sort question #3032 step 2 is about.
	ordered := make([]int64, len(interleaved))
	for i := range ordered {
		ordered[i] = base + int64(i)*1000
	}
	op, od := write(t, false, ordered), write(t, true, ordered)
	t.Logf("MEASURED (perfectly ordered + zstd): PLAIN=%d delta=%d (%.2fx)", op, od, float64(op)/float64(od))
	t.Logf("MEASURED (host-sorted shape, zstd both sides, %d hosts x %d events): PLAIN=%d delta=%d (%.2fx)",
		hosts, perHost, plain, delta, float64(plain)/float64(delta))

	assert.Less(t, delta, plain,
		"delta must not be a PESSIMISATION on the interleaved shape the lake actually writes (plain=%d delta=%d)", plain, delta)
}

func TestParquetBatchSizeFromTableProperties(t *testing.T) {
	t.Run("default batch size when no properties in context", func(t *testing.T) {
		ctx := context.Background()
		props := internal.TablePropertiesFromContext(ctx)
		assert.Equal(t, internal.ParquetBatchSizeDefault, props.GetInt(internal.ParquetBatchSizeKey, internal.ParquetBatchSizeDefault))
	})

	t.Run("custom batch size from context properties", func(t *testing.T) {
		customBatchSize := 1024
		props := iceberg.Properties{internal.ParquetBatchSizeKey: "1024"}
		ctx := internal.WithTableProperties(context.Background(), props)
		got := internal.TablePropertiesFromContext(ctx)
		assert.Equal(t, customBatchSize, got.GetInt(internal.ParquetBatchSizeKey, internal.ParquetBatchSizeDefault))
	})

	t.Run("invalid value falls back to default", func(t *testing.T) {
		props := iceberg.Properties{internal.ParquetBatchSizeKey: "not-a-number"}
		ctx := internal.WithTableProperties(context.Background(), props)
		got := internal.TablePropertiesFromContext(ctx)
		assert.Equal(t, internal.ParquetBatchSizeDefault, got.GetInt(internal.ParquetBatchSizeKey, internal.ParquetBatchSizeDefault))
	})
}

// buildBloomTestParquet writes a Parquet file with two row groups into buf.
// The single required INT32 column "id" has Iceberg field_id=1 and bloom
// filters enabled. RG0 holds values [1..rgSize], RG1 holds [rgSize+1..2*rgSize].
func buildBloomTestParquet(t *testing.T, rgSize int) []byte {
	t.Helper()

	idNode := schema.NewInt32Node("id", parquet.Repetitions.Required, 1)

	rootNode, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{idNode}, -1)
	require.NoError(t, err)

	writerProps := parquet.NewWriterProperties(
		parquet.WithStats(true),
		parquet.WithBloomFilterEnabledFor("id", true),
	)

	var buf bytes.Buffer
	pw := file.NewParquetWriter(&buf, rootNode, file.WithWriterProps(writerProps))

	writeRG := func(start, end int) {
		rgw := pw.AppendRowGroup()
		cw, werr := rgw.NextColumn()
		require.NoError(t, werr)

		vals := make([]int32, end-start+1)
		for i := range vals {
			vals[i] = int32(start + i)
		}

		_, werr = cw.(*file.Int32ColumnChunkWriter).WriteBatch(vals, nil, nil)
		require.NoError(t, werr)
		require.NoError(t, cw.Close())
		require.NoError(t, rgw.Close())
	}

	writeRG(1, rgSize)          // RG0
	writeRG(rgSize+1, 2*rgSize) // RG1

	require.NoError(t, pw.Close())

	return buf.Bytes()
}

// openBloomTestReader creates a fresh FileReader from raw Parquet bytes for
// each subtest call, so row-group state is not shared between subtests.
func openBloomTestReader(t *testing.T, data []byte) internal.FileReader {
	t.Helper()

	pqRdr, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)

	arrRdr, err := pqarrow.NewFileReader(pqRdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)

	return internal.WrapParquetFileReader(arrRdr)
}

func int32PhysBytes(v int32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))

	return b
}

func countRecords(t *testing.T, rr array.RecordReader) int64 {
	t.Helper()
	defer rr.Release()

	var n int64
	for rr.Next() {
		n += rr.RecordBatch().NumRows()
	}

	return n
}

// TestBloomFilterRowGroupPruning verifies that ParquetRowGroupTester's bloom
// filter pass correctly skips row groups that definitively do not contain a
// queried value, while keeping row groups where the value is present.
func TestBloomFilterRowGroupPruning(t *testing.T) {
	const rgSize = 100 // 100 rows per row group

	data := buildBloomTestParquet(t, rgSize)

	// Stats function that always keeps the row group (isolates bloom logic).
	alwaysKeep := func(_ *metadata.RowGroupMetaData, _ []int) (bool, error) {
		return true, nil
	}

	ctx := context.Background()
	cols := []int{0}

	t.Run("value in RG0 prunes RG1", func(t *testing.T) {
		rdr := openBloomTestReader(t, data)
		tester := &internal.ParquetRowGroupTester{
			StatsFn: alwaysKeep,
			BloomPreds: []internal.RowGroupBloomPred{
				{FieldID: 1, PhysBytes: [][]byte{int32PhysBytes(50)}}, // 50 is in RG0
			},
		}
		rr, err := rdr.GetRecords(ctx, cols, tester)
		require.NoError(t, err)

		assert.Equal(t, int64(rgSize), countRecords(t, rr), "only RG0 rows expected")
	})

	t.Run("value in RG1 prunes RG0", func(t *testing.T) {
		rdr := openBloomTestReader(t, data)
		tester := &internal.ParquetRowGroupTester{
			StatsFn: alwaysKeep,
			BloomPreds: []internal.RowGroupBloomPred{
				{FieldID: 1, PhysBytes: [][]byte{int32PhysBytes(150)}}, // 150 is in RG1
			},
		}
		rr, err := rdr.GetRecords(ctx, cols, tester)
		require.NoError(t, err)

		assert.Equal(t, int64(rgSize), countRecords(t, rr), "only RG1 rows expected")
	})

	t.Run("value absent from all row groups skips all", func(t *testing.T) {
		rdr := openBloomTestReader(t, data)
		tester := &internal.ParquetRowGroupTester{
			StatsFn: alwaysKeep,
			BloomPreds: []internal.RowGroupBloomPred{
				{FieldID: 1, PhysBytes: [][]byte{int32PhysBytes(999)}}, // 999 not in either RG
			},
		}
		rr, err := rdr.GetRecords(ctx, cols, tester)
		require.NoError(t, err)

		assert.Equal(t, int64(0), countRecords(t, rr), "no rows expected when value absent")
	})

	t.Run("In predicate with a value in each RG keeps both", func(t *testing.T) {
		rdr := openBloomTestReader(t, data)
		tester := &internal.ParquetRowGroupTester{
			StatsFn: alwaysKeep,
			BloomPreds: []internal.RowGroupBloomPred{
				{
					FieldID: 1,
					PhysBytes: [][]byte{
						int32PhysBytes(50),  // in RG0
						int32PhysBytes(150), // in RG1
					},
				},
			},
		}
		rr, err := rdr.GetRecords(ctx, cols, tester)
		require.NoError(t, err)

		assert.Equal(t, int64(2*rgSize), countRecords(t, rr), "both row groups expected")
	})

	t.Run("nil bloom preds reads all row groups", func(t *testing.T) {
		rdr := openBloomTestReader(t, data)
		tester := &internal.ParquetRowGroupTester{
			StatsFn:    alwaysKeep,
			BloomPreds: nil,
		}
		rr, err := rdr.GetRecords(ctx, cols, tester)
		require.NoError(t, err)

		assert.Equal(t, int64(2*rgSize), countRecords(t, rr), "all rows expected without bloom preds")
	})

	t.Run("unknown field ID is ignored and row group kept", func(t *testing.T) {
		rdr := openBloomTestReader(t, data)
		tester := &internal.ParquetRowGroupTester{
			StatsFn: alwaysKeep,
			BloomPreds: []internal.RowGroupBloomPred{
				{FieldID: 999, PhysBytes: [][]byte{int32PhysBytes(50)}}, // field 999 does not exist
			},
		}
		rr, err := rdr.GetRecords(ctx, cols, tester)
		require.NoError(t, err)

		assert.Equal(t, int64(2*rgSize), countRecords(t, rr), "all rows expected when field ID unknown")
	})
}
