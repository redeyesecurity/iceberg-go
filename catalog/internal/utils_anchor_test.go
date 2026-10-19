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

package internal

import (
	"context"
	"path"
	"testing"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The catalogs built on UpdateAndStageTable place the new metadata file from
// the base's location, so a base whose location was planted to another
// table's directory is refused before anything is staged (caver-go#9536).
func TestUpdateAndStageTableHoldsTheWriteToTheLoadedDirectory9536(t *testing.T) {
	ident := table.Identifier{"db", "t"}
	fsF := func(context.Context) (icebergio.IO, error) { return icebergio.LocalFS{}, nil }
	current := func(t *testing.T, loc string) *table.Table {
		t.Helper()
		meta, err := table.NewMetadata(iceberg.NewSchema(0), iceberg.UnpartitionedSpec,
			table.UnsortedSortOrder, loc, iceberg.Properties{table.PropertyFormatVersion: "2"})
		require.NoError(t, err)

		return table.New(ident, meta, "/w/t/metadata/00001-a.metadata.json", fsF, nil)
	}

	staged, err := UpdateAndStageTable(t.Context(), current(t, "/w/t"), ident, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "/w/t/metadata", path.Dir(staged.MetadataLocation()))

	_, err = UpdateAndStageTable(t.Context(), current(t, "/w/hot"), ident, nil, nil, nil)
	require.ErrorIs(t, err, table.ErrMetadataWriteRefused)
}
