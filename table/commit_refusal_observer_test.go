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
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// observingCatalog is a placingCatalog that records every refusal doCommit
// reports to it.
type observingCatalog struct {
	*placingCatalog
	idents  []Identifier
	refused []error
}

func (c *observingCatalog) CommitRefused(_ context.Context, ident Identifier, err error) {
	c.idents = append(c.idents, ident)
	c.refused = append(c.refused, err)
}

// commit loads the table from c and commits once, so doCommit reports to c.
func (c *observingCatalog) commit(t *testing.T) (*Table, error) {
	t.Helper()
	tbl, err := c.LoadTable(t.Context(), Identifier{"db", "t"})
	require.NoError(t, err)

	return New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.fsF, c).
		doCommit(t.Context(), nil, nil)
}

// A plant that moves the location is refused before CommitTable runs, and the
// catalog is told: once per refused commit, with the table and the same error
// the caller gets back (caver-go#9632).
func TestAnAnchorRefusalIsReportedToTheCatalog9632(t *testing.T) {
	f := newAnchorFixture(t, nil)
	cat := &observingCatalog{placingCatalog: f.cat}
	f.plantInPlace(t, f.wh+"/hot", f.victimHead)

	for i := 1; i <= 2; i++ {
		_, err := cat.commit(t)
		require.ErrorIs(t, err, ErrMetadataWriteRefused, "commit %d", i)
		require.Len(t, cat.refused, i, "commit %d is reported once", i)
		assert.Same(t, err, cat.refused[i-1], "the catalog sees the error the caller gets")
		assert.Equal(t, Identifier{"db", "t"}, cat.idents[i-1])
	}
	assert.Zero(t, f.cat.attempts, "the refused commits never reach CommitTable")
}

// A refusal on a retry, after the base was refreshed onto a plant, is reported
// once, not once per attempt.
func TestARetryRefusalIsReportedOnce9632(t *testing.T) {
	f := newAnchorFixture(t, iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat := &observingCatalog{placingCatalog: f.cat}
	f.cat.failFirst = 1
	f.cat.onFail = func(c *placingCatalog) {
		c.metadata = plantedMetadata(t, c.metadata, f.wh+"/hot", f.victimHead)
	}

	_, err := cat.commit(t)
	require.ErrorIs(t, err, ErrMetadataWriteRefused)
	assert.Len(t, cat.refused, 1)
	assert.Equal(t, 1, f.cat.attempts)
}

// An honest commit and a commit the catalog itself fails report nothing: the
// observer hears only about refusals the catalog never saw.
func TestOnlyAnAnchorRefusalIsReported9632(t *testing.T) {
	f := newAnchorFixture(t, nil)
	cat := &observingCatalog{placingCatalog: f.cat}
	_, err := cat.commit(t)
	require.NoError(t, err)
	assert.Empty(t, cat.refused)

	f.cat.failFirst = f.cat.attempts + 1 // the next attempt loses a race, with no retry left
	_, err = cat.commit(t)
	require.ErrorIs(t, err, ErrCommitFailed)
	assert.Empty(t, cat.refused)
}

// A catalog that does not implement CommitRefusalObserver still gets the
// refusal returned, as before.
func TestARefusalWithoutAnObserverIsStillReturned9632(t *testing.T) {
	f := newAnchorFixture(t, nil)
	f.plantInPlace(t, f.wh+"/hot")
	_, ok := CatalogIO(f.cat).(CommitRefusalObserver)
	require.False(t, ok)

	_, err := f.commit(t, &removeRecordingIO{})
	require.ErrorIs(t, err, ErrMetadataWriteRefused)
}
