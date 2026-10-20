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
	"errors"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// observingCatalog is a placingCatalog that implements CommitRefusalObserver
// and records each refusal it is told of.
type observingCatalog struct {
	*placingCatalog
	idents []Identifier
	errs   []error
}

func (c *observingCatalog) ObserveCommitRefusal(_ context.Context, ident Identifier, err error) {
	c.idents = append(c.idents, ident)
	c.errs = append(c.errs, err)
}

func (f anchorFixture) observedCommit(t *testing.T, cat *observingCatalog) (*Table, error) {
	t.Helper()
	tbl := New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location,
		func(context.Context) (iceio.IO, error) { return &removeRecordingIO{}, nil }, cat)

	return tbl.doCommit(t.Context(), nil, nil)
}

// A commit doCommit refuses on a moved base never reaches CommitTable, so the
// catalog is told of it instead (caver-go#9632): once per refused commit, with
// the table's identifier and the error the caller gets.
func TestAMetadataWriteRefusalIsObservedByTheCatalog9632(t *testing.T) {
	f := newAnchorFixture(t, nil)
	f.plantInPlace(t, f.wh+"/hot", f.victimHead)
	cat := &observingCatalog{placingCatalog: f.cat}

	for i := 1; i <= 2; i++ {
		_, err := f.observedCommit(t, cat)
		require.ErrorIs(t, err, ErrMetadataWriteRefused, "commit %d", i)
		require.Len(t, cat.errs, i, "commit %d is observed once", i)
		assert.Same(t, err, cat.errs[i-1], "the observer sees the error the caller gets")
		assert.Equal(t, Identifier{"db", "t"}, cat.idents[i-1])
	}
	assert.Zero(t, f.cat.attempts, "a refused commit never reaches CommitTable")
}

// A retry refused on the base it refreshed to is observed once, and the lost
// race before it, which CommitTable saw, is not.
func TestARetryRefusalIsObservedOnce9632(t *testing.T) {
	f := newAnchorFixture(t, iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	victimHead := f.victimHead
	f.cat.failFirst = 1
	f.cat.onFail = func(c *placingCatalog) {
		c.metadata = plantedMetadata(t, c.metadata, f.wh+"/hot", victimHead)
	}
	cat := &observingCatalog{placingCatalog: f.cat}

	_, err := f.observedCommit(t, cat)
	require.ErrorIs(t, err, ErrMetadataWriteRefused)
	require.Len(t, cat.errs, 1)
	assert.Same(t, err, cat.errs[0])
	assert.Equal(t, 1, f.cat.attempts)
}

// An honest commit lands and the observer hears nothing.
func TestAnHonestCommitIsNotObserved9632(t *testing.T) {
	f := newAnchorFixture(t, nil)
	cat := &observingCatalog{placingCatalog: f.cat}

	_, err := f.observedCommit(t, cat)
	require.NoError(t, err)
	assert.Empty(t, cat.errs)
	assert.Equal(t, 1, f.cat.commits)
}

// failingCatalog is an observingCatalog whose CommitTable fails every attempt
// with err, as a catalog that answers 5xx or refuses the commit itself does.
type failingCatalog struct {
	*observingCatalog
	err error
}

func (c *failingCatalog) CommitTable(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error) {
	c.attempts++

	return nil, "", c.err
}

// A commit CommitTable fails reaches the catalog, so the observer hears
// nothing of it, whatever the error (caver-go#9654). That holds for an
// unknown-state error, for a catalog-side refusal that wraps
// ErrMetadataWriteRefused (an observer counting both would count it twice),
// and for a lost race with no retry left.
func TestACommitTableErrorIsNotObserved9654(t *testing.T) {
	for name, tc := range map[string]struct {
		err   error
		props iceberg.Properties
	}{
		"unknown state": {err: errors.New("catalog answered 503")},
		"catalog-side refusal": {
			err: fmt.Errorf("catalog belt: %w", ErrMetadataWriteRefused),
		},
		"lost race, retries exhausted": {
			err: fmt.Errorf("pointer race: %w", ErrCommitFailed),
			props: iceberg.Properties{
				CommitNumRetriesKey:     "1",
				CommitMinRetryWaitMsKey: "1",
				CommitMaxRetryWaitMsKey: "2",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newAnchorFixture(t, tc.props)
			obs := &observingCatalog{placingCatalog: f.cat}
			cat := &failingCatalog{observingCatalog: obs, err: tc.err}
			tbl := New(Identifier{"db", "t"}, f.cat.metadata, f.cat.location,
				func(context.Context) (iceio.IO, error) { return &removeRecordingIO{}, nil }, cat)

			_, err := tbl.doCommit(t.Context(), nil, nil)
			require.ErrorIs(t, err, tc.err)
			assert.Positive(t, f.cat.attempts, "the commit reached CommitTable")
			assert.Empty(t, obs.errs, "a commit the catalog saw is not observed")
		})
	}
}
