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
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/gcerrors"
)

// removeRecordingIO is LocalFS that records every Remove it is asked for.
type removeRecordingIO struct {
	iceio.LocalFS
	mu      sync.Mutex
	removed []string
}

func (r *removeRecordingIO) Remove(name string) error {
	r.mu.Lock()
	r.removed = append(r.removed, name)
	r.mu.Unlock()

	return r.LocalFS.Remove(name)
}

// oneVersionLogCatalog keeps a metadata log of one entry, the file the commit
// replaced, the way write.metadata.previous-versions-max=1 would. Its first
// failFirst commits are refused with ErrCommitFailed, as a lost pointer race.
type oneVersionLogCatalog struct {
	dir       string
	metadata  Metadata
	location  string
	version   int
	failFirst int32
	attempts  atomic.Int32
}

func (c *oneVersionLogCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	return New(ident, c.metadata, c.location,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *oneVersionLogCatalog) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	if c.attempts.Add(1) <= c.failFirst {
		return nil, "", fmt.Errorf("pointer race: %w", ErrCommitFailed)
	}

	return c.commit(updates)
}

// commit lands a new version on the catalog's current one and writes its file.
func (c *oneVersionLogCatalog) commit(updates []Update) (Metadata, string, error) {
	bldr, err := MetadataBuilderFromBase(c.metadata, "")
	if err != nil {
		return nil, "", err
	}
	bldr.TrimMetadataLogs(0)
	bldr.AppendMetadataLog(MetadataLogEntry{MetadataFile: c.location, TimestampMs: time.Now().UnixMilli()})
	for _, u := range updates {
		if err := u.Apply(bldr); err != nil {
			return nil, "", err
		}
	}
	meta, err := bldr.Build()
	if err != nil {
		return nil, "", err
	}

	c.version++
	// The files live in <location>/metadata, the only directory the cleanup
	// will delete from (caver-go#9525).
	if err := os.MkdirAll(filepath.Join(c.dir, "metadata"), 0o755); err != nil {
		return nil, "", err
	}
	loc := filepath.ToSlash(filepath.Join(c.dir, "metadata", fmt.Sprintf("v%d.metadata.json", c.version)))
	if err := os.WriteFile(loc, []byte("{}"), 0o644); err != nil {
		return nil, "", err
	}
	c.metadata, c.location = meta, loc

	return meta, loc, nil
}

// A writer whose commit loses a race and lands on the retry must clean up
// against the base it actually landed on (caver-go#9384). Its stale view still
// names v1, which the peer's commit already trimmed and deleted, and before
// the fix it removed v1 a second time and left v2, the file its own commit
// trimmed, behind for good.
func TestDeleteOldMetadataUsesTheBaseTheRetryLandedOn(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, dir, iceberg.Properties{
		PropertyFormatVersion:               "2",
		MetadataDeleteAfterCommitEnabledKey: "true",
		CommitNumRetriesKey:                 "3",
		CommitMinRetryWaitMsKey:             "1",
		CommitMaxRetryWaitMsKey:             "2",
	})
	require.NoError(t, err)

	cat := &oneVersionLogCatalog{dir: dir, metadata: meta}
	_, v1, err := cat.commit(nil)
	require.NoError(t, err)
	_, v2, err := cat.commit(nil)
	require.NoError(t, err)

	// The writer's view is v2, whose log names v1.
	fsys := &removeRecordingIO{}
	stale := New(Identifier{"db", "t"}, cat.metadata, cat.location,
		func(context.Context) (iceio.IO, error) { return fsys, nil }, cat)

	// A peer lands v3, trims v1 from the log and deletes it.
	_, _, err = cat.commit(nil)
	require.NoError(t, err)
	require.NoError(t, os.Remove(v1))

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	cat.failFirst = cat.attempts.Load() + 1
	_, err = stale.doCommit(t.Context(), nil, nil)
	require.NoError(t, err)

	assert.Equal(t, []string{v2}, fsys.removed, "only the file this commit trimmed is removed")
	assert.NoFileExists(t, v2)
	assert.NotContains(t, logBuf.String(), "Failed to delete old metadata file")
}

func TestIsNotExist(t *testing.T) {
	_, statErr := os.Stat(filepath.Join(t.TempDir(), "absent"))
	assert.True(t, isNotExist(statErr), "an os.PathError wrapping ENOENT")

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	blobErr := bucket.Delete(t.Context(), "absent")
	require.Equal(t, gcerrors.NotFound, gcerrors.Code(blobErr))
	assert.True(t, isNotExist(blobErr), "a gocloud NotFound, which does not wrap fs.ErrNotExist")

	assert.False(t, isNotExist(fmt.Errorf("permission denied")))
	assert.False(t, isNotExist(nil))
}
