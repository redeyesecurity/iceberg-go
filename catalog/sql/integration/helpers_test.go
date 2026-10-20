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

//go:build integration

package integration_test

import (
	"crypto/rand"
	"encoding/hex"
	"strings"

	"context"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/sql/internal/testhooks"
)

// databaseName is a fresh lower-case database name per call, the shape the sql
// catalog's own suite uses (catalog/sql/sql_test.go); copied here because this
// module cannot reach that test package's helpers.
func databaseName() string {
	var b [10]byte
	_, _ = rand.Read(b[:])

	return strings.ToLower("my-iceberg-db-" + hex.EncodeToString(b[:]))
}

// tableName is a fresh lower-case table name per call, same shape as the sql
// catalog suite's helper.
func tableName() string {
	var b [10]byte
	_, _ = rand.Read(b[:])

	return strings.ToLower("my-iceberg-table-" + hex.EncodeToString(b[:]))
}

// tableSchemaNested is the schema the sql catalog suite creates tables with,
// copied verbatim from catalog/sql/sql_test.go.
var tableSchemaNested = iceberg.NewSchemaWithIdentifiers(1,
	[]int{1},
	iceberg.NestedField{
		ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false,
	},
	iceberg.NestedField{
		ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	},
	iceberg.NestedField{
		ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false,
	},
	iceberg.NestedField{
		ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
			ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true,
		},
	},
	iceberg.NestedField{
		ID: 6, Name: "quux",
		Type: &iceberg.MapType{
			KeyID:   7,
			KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 8,
			ValueType: &iceberg.MapType{
				KeyID:         9,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       10,
				ValueType:     iceberg.PrimitiveTypes.Int32,
				ValueRequired: true,
			},
			ValueRequired: true,
		},
		Required: true,
	},
	iceberg.NestedField{
		ID: 11, Name: "location", Type: &iceberg.ListType{
			ElementID: 12, Element: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
				},
			},
			ElementRequired: true,
		},
		Required: true,
	},
	iceberg.NestedField{
		ID:   15,
		Name: "person",
		Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 17, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			},
		},
		Required: false,
	},
)

// setCreateCheckedHook installs fn as the hook a create runs between its
// in-transaction held-directory check and its insert, and returns a func that
// removes it. Same seam as catalog/sql's own SetCreateCheckedHook, which lives
// in that package's export_test.go and is out of reach from this module.
func setCreateCheckedHook(fn func(context.Context)) (restore func()) {
	testhooks.CreateChecked = fn

	return func() { testhooks.CreateChecked = nil }
}
