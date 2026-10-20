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

// PartitionField.EscapedName used to fill its escapedName cache on first use with
// a plain write, so the writers formatting partition paths concurrently
// (PartitionToPath on every data-writer goroutine) raced each other and raced the
// by-value copy PartitionField.MarshalJSON makes when a commit serialises the
// spec (caver-go#9669, the race the issue named). The cache is now filled when
// the spec is built and never written after; here a path is formatted while the
// spec is marshalled, on fresh specs, many times, and the outputs are the ones the
// first run produced. Under -race the old shape fails this test.
func TestPartitionPathAndSpecMarshalDoNotRaceOnTheEscapedName(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "other_str", Type: iceberg.PrimitiveTypes.String},
	)
	newSpec := func() iceberg.PartitionSpec {
		return iceberg.NewPartitionSpecID(1,
			iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "my#str%bucket"},
			iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Transform: iceberg.IdentityTransform{}, Name: "other str+bucket"},
		)
	}
	record := partitionRecord{"my+str", "( )"}
	first := newSpec()
	wantPath := first.PartitionToPath(record, schema)
	require.Equal(t, "my%23str%25bucket=my%2Bstr/other+str%2Bbucket=%28+%29", wantPath)
	wantJSON, err := json.Marshal(&first)
	require.NoError(t, err)

	for i := 0; i < 300; i++ {
		spec := newSpec()
		var wg sync.WaitGroup
		wg.Add(2)
		var gotPath string
		var gotJSON []byte
		var merr error
		go func() {
			defer wg.Done()
			gotPath = spec.PartitionToPath(record, schema)
		}()
		go func() {
			defer wg.Done()
			gotJSON, merr = json.Marshal(&spec)
		}()
		wg.Wait()
		require.NoError(t, merr)
		require.Equal(t, wantPath, gotPath)
		require.Equal(t, string(wantJSON), string(gotJSON))
	}
}
