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

package iceberg_test

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// Schema.MarshalJSON used to copy the whole Schema by value to borrow its json
// tags, which read the lazily published index pointers (atomic.Pointer fields)
// non-atomically while another goroutine was storing them: the race detector
// flagged a commit's metadata write against a concurrent stats collector on the
// same schema (caver-go#9669). Marshalling and a lazy lookup now run together on
// one schema, many times; under -race the old shape fails this test, and the
// bytes produced are the ones produced before.
func TestSchemaMarshalJSONDoesNotCopyTheLazyIndexes(t *testing.T) {
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	}
	want, err := json.Marshal(iceberg.NewSchemaWithIdentifiers(3, []int{1}, fields...))
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"struct","fields":[{"id":1,"name":"id","type":"long","required":true},{"id":2,"name":"name","type":"string","required":false}],"schema-id":3,"identifier-field-ids":[1]}`, string(want))

	for i := 0; i < 300; i++ {
		s := iceberg.NewSchemaWithIdentifiers(3, []int{1}, fields...)
		var wg sync.WaitGroup
		wg.Add(2)
		var got []byte
		var merr error
		go func() {
			defer wg.Done()
			got, merr = json.Marshal(s)
		}()
		go func() {
			defer wg.Done()
			_, _ = s.FindColumnName(2)
			_, _ = s.FindFieldByName("id")
		}()
		wg.Wait()
		require.NoError(t, merr)
		require.Equal(t, string(want), string(got), "the marshalled bytes changed")
	}
}
