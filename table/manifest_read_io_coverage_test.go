package table

// manifest_read_io_coverage_test.go: every manifest list and manifest the commit
// path opens goes through the table's ManifestReadIO, door by door (caver-go#9520,
// fork PR #17 review F1, F2, F4, F7). The raw FileIO records every open it serves;
// the hook records every open it is asked for; a manifest or list the raw FileIO
// opened that the hook never saw is a read past the hook, whichever producer made it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// spyFS is LocalFS recording every path opened through it.
type spyFS struct {
	iceio.LocalFS
	mu     sync.Mutex
	opened []string
}

func (s *spyFS) Open(name string) (iceio.File, error) {
	s.mu.Lock()
	s.opened = append(s.opened, name)
	s.mu.Unlock()

	return s.LocalFS.Open(name)
}

func (s *spyFS) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.opened
	s.opened = nil

	return out
}

// hookSpy is the ManifestReadIO wrapper: it records what the commit path asked it
// for and forwards to the inner FileIO (so the raw spy sees the same open).
type hookSpy struct {
	inner  iceio.IO
	mu     sync.Mutex
	opened []string
	refuse string
}

var errHookRefused = errors.New("refused by the hook")

func (h *hookSpy) Open(name string) (iceio.File, error) {
	h.mu.Lock()
	h.opened = append(h.opened, name)
	h.mu.Unlock()
	if h.refuse != "" && strings.Contains(name, h.refuse) {
		return nil, fmt.Errorf("%w: %s", errHookRefused, name)
	}

	return h.inner.Open(name)
}

func (h *hookSpy) Remove(string) error { return errors.New("a manifest reader does not remove") }

func (h *hookSpy) take() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.opened
	h.opened = nil

	return out
}

// hookCat is a catalog that loads tables through the test's FileIO and can fail the
// first attempts with a given error, for the retry path.
type hookCat struct {
	meta      Metadata
	fsF       FSysF
	failUntil int
	failWith  error
	attempts  atomic.Int32
}

func (c *hookCat) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	return New(ident, c.meta, c.meta.Location()+"/metadata/current.json", c.fsF, c), nil
}

func (c *hookCat) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	if int(c.attempts.Add(1)) <= c.failUntil {
		return nil, "", c.failWith
	}

	return c.apply(updates)
}

// apply lands updates and answers the new metadata location the way a real catalog
// does; the expire cleanup anchors its removes on it.
func (c *hookCat) apply(updates []Update) (Metadata, string, error) {
	meta, err := UpdateTableMetadata(c.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.meta = meta

	return meta, fmt.Sprintf("%s/metadata/%05d.metadata.json", meta.Location(), c.attempts.Load()), nil
}

type coverageRig struct {
	tbl   *Table
	raw   *spyFS
	hook  *hookSpy
	cat   *hookCat
	write func(path string)
	n     int
}

func newCoverageRig(t *testing.T, props iceberg.Properties) *coverageRig {
	t.Helper()
	dir := filepath.ToSlash(t.TempDir())
	raw := &spyFS{}
	hook := &hookSpy{}
	fsF := func(context.Context) (iceio.IO, error) { return raw, nil }
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false}}, nil)
	write := func(path string) {
		bldr := array.NewInt32Builder(memory.DefaultAllocator)
		defer bldr.Release()
		bldr.AppendValues([]int32{1}, nil)
		col := bldr.NewArray()
		defer col.Release()
		rec := array.NewRecordBatch(arrowSchema, []arrow.Array{col}, 1)
		defer rec.Release()
		arrTbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
		defer arrTbl.Release()
		fo, err := raw.Create(path)
		require.NoError(t, err)
		require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(), nil, pqarrow.DefaultWriterProps()))
	}
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	merged := iceberg.Properties{PropertyFormatVersion: "2"}
	for k, v := range props {
		merged[k] = v
	}
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, dir, merged)
	require.NoError(t, err)
	cat := &hookCat{meta: meta, fsF: fsF}
	tbl := New(Identifier{"default", "cov"}, meta, dir+"/metadata/00000.json", fsF, cat)
	tbl = tbl.WithManifestReadIO(func(fs iceio.IO) iceio.IO { hook.inner = fs; return hook })

	return &coverageRig{tbl: tbl, raw: raw, hook: hook, cat: cat, write: write}
}

// next is a fresh parquet file under the table.
func (r *coverageRig) next() string {
	r.n++
	p := fmt.Sprintf("%s/data-%d.parquet", r.tbl.Location(), r.n)
	r.write(p)

	return p
}

func (r *coverageRig) appendOne(t *testing.T) {
	t.Helper()
	txn := r.tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(context.Background(), []string{r.next()}, nil, false))
	landed, err := txn.Commit(context.Background())
	require.NoError(t, err)
	r.tbl = landed
}

// avroOpens is the manifest lists and manifests among paths, as a sorted multiset.
func avroOpens(paths []string) []string {
	var out []string
	for _, p := range paths {
		if strings.HasSuffix(p, ".avro") {
			out = append(out, p)
		}
	}
	sort.Strings(out)

	return out
}

// requireEveryManifestOpenHooked: what the raw FileIO opened among lists and
// manifests is exactly what the hook was asked for (the hook forwards, so the
// counts match; an open past the hook shows as a raw open with no hook open).
func requireEveryManifestOpenHooked(t *testing.T, r *coverageRig, door string) {
	t.Helper()
	raw, hooked := avroOpens(r.raw.take()), avroOpens(r.hook.take())
	require.NotEmpty(t, raw, "%s: the door opened no manifest list or manifest at all, so this proves nothing", door)
	require.Equal(t, raw, hooked, "%s: a manifest list or manifest was opened past the hook", door)
}

func TestEveryCommitPathManifestReadGoesThroughTheHook(t *testing.T) {
	ctx := context.Background()
	t.Run("fast append and the AddFiles duplicate check", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		r.raw.take()
		r.hook.take()
		r.appendOne(t)
		requireEveryManifestOpenHooked(t, r, "fast append")
	})
	t.Run("merge append", func(t *testing.T) {
		r := newCoverageRig(t, iceberg.Properties{ManifestMergeEnabledKey: "true", ManifestMinMergeCountKey: "2"})
		r.appendOne(t)
		r.appendOne(t)
		r.raw.take()
		r.hook.take()
		r.appendOne(t) // merges the parent's manifests: their entries are read
		requireEveryManifestOpenHooked(t, r, "merge append")
	})
	t.Run("overwrite through ReplaceDataFiles", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		old := fmt.Sprintf("%s/data-%d.parquet", r.tbl.Location(), r.n)
		r.raw.take()
		r.hook.take()
		txn := r.tbl.NewTransaction()
		require.NoError(t, txn.ReplaceDataFiles(ctx, []string{old}, []string{r.next()}, nil))
		landed, err := txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "overwrite")
	})
	t.Run("rewrite manifests", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		r.appendOne(t)
		r.raw.take()
		r.hook.take()
		txn := r.tbl.NewTransaction()
		_, err := txn.RewriteManifests(ctx, nil)
		require.NoError(t, err)
		landed, err := txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "rewrite manifests")
	})
	t.Run("expire snapshots cleanup", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		r.appendOne(t)
		time.Sleep(5 * time.Millisecond) // the older snapshot is older than 0 ms by a full tick
		r.raw.take()
		r.hook.take()
		txn := r.tbl.NewTransaction()
		require.NoError(t, txn.ExpireSnapshots(WithRetainLast(1), WithOlderThan(0), WithPostCommit(true)))
		landed, err := txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "expire snapshots")
	})
	t.Run("the rebuild on a commit retry", func(t *testing.T) {
		r := newCoverageRig(t, iceberg.Properties{CommitNumRetriesKey: "4", CommitMinRetryWaitMsKey: "1", CommitMaxRetryWaitMsKey: "2"})
		r.appendOne(t)
		r.cat.failUntil, r.cat.failWith = int(r.cat.attempts.Load())+1, fmt.Errorf("REST: %w", ErrCommitFailed)
		r.raw.take()
		r.hook.take()
		r.appendOne(t) // the first attempt fails, the rebuild re-reads the fresh parent
		require.GreaterOrEqual(t, int(r.cat.attempts.Load()), 3, "the retry did not happen")
		requireEveryManifestOpenHooked(t, r, "commit retry")
	})
	t.Run("conflict validation on a concurrent snapshot", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		base := r.tbl
		old := fmt.Sprintf("%s/data-%d.parquet", r.tbl.Location(), r.n)
		r.appendOne(t) // a concurrent commit moves main past base
		r.raw.take()
		r.hook.take()
		// An overwrite from the stale base registers a validator; doCommit walks the
		// concurrent snapshot's list and manifests before it commits.
		txn := base.NewTransaction()
		require.NoError(t, txn.ReplaceDataFiles(ctx, []string{old}, []string{r.next()}, nil))
		// The pre-flight validation runs against the refreshed head before CommitTable;
		// the test catalog checks no requirements, so the stale commit itself then fails
		// on the sequence number, which is not what this door is about.
		_, _ = txn.Commit(ctx)
		requireEveryManifestOpenHooked(t, r, "conflict validation")
	})
}

// The table Commit returns carries the hook: a caller that chains on it reads
// through the hook on the next commit (review F4).
func TestTheTableACommitReturnsKeepsTheHook(t *testing.T) {
	r := newCoverageRig(t, nil)
	r.appendOne(t) // r.tbl is now the table Commit returned
	r.raw.take()
	r.hook.take()
	r.hook.refuse = "snap-"
	txn := r.tbl.NewTransaction()
	err := txn.AddFiles(context.Background(), []string{r.next()}, nil, false)
	if err == nil {
		_, err = txn.Commit(context.Background())
	}
	require.ErrorIs(t, err, errHookRefused, "the table Commit returned lost the hook")
}

// strictCat is hookCat with the catalog's own requirement check: a commit whose
// AssertRefSnapshotID no longer holds fails with ErrCommitFailed, so the client
// refreshes, rebuilds against the moved parent and runs its validators, the way a
// real catalog makes it. Every attempt counts, the refused ones included.
type strictCat struct{ hookCat }

func (c *strictCat) LoadTable(ctx context.Context, ident Identifier) (*Table, error) {
	return New(ident, c.meta, c.meta.Location()+"/metadata/current.json", c.fsF, c), nil
}

func (c *strictCat) CommitTable(_ context.Context, _ Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	c.attempts.Add(1)
	for _, r := range reqs {
		if err := r.Validate(c.meta); err != nil {
			return nil, "", fmt.Errorf("%w: %v", ErrCommitFailed, err)
		}
	}

	return c.apply(updates)
}

func newStrictRig(t *testing.T, props iceberg.Properties) *coverageRig {
	t.Helper()
	r := newCoverageRig(t, props)
	sc := &strictCat{hookCat: hookCat{meta: r.cat.meta, fsF: r.cat.fsF}}
	r.tbl = New(r.tbl.Identifier(), r.tbl.Metadata(), r.tbl.MetadataLocation(), r.cat.fsF, sc).
		WithManifestReadIO(func(fs iceio.IO) iceio.IO { r.hook.inner = fs; return r.hook })
	r.cat = &sc.hookCat

	return r
}

// dataFilesFor is the DataFile descriptors AddFiles would build for paths.
func (r *coverageRig) dataFilesFor(t *testing.T, paths ...string) []iceberg.DataFile {
	t.Helper()
	mb, err := MetadataBuilderFromBase(r.tbl.Metadata(), r.tbl.MetadataLocation())
	require.NoError(t, err)
	dfs, err := filesToDataFiles(context.Background(), r.raw, mb, paths, 1)
	require.NoError(t, err)

	return dfs
}

// currentDataFiles is the data files the current snapshot holds, through the hook.
func (r *coverageRig) currentDataFiles(t *testing.T) []iceberg.DataFile {
	t.Helper()
	var out []iceberg.DataFile
	for df, err := range r.tbl.CurrentSnapshot().dataFiles(r.hook, nil) {
		require.NoError(t, err)
		out = append(out, df)
	}
	r.raw.take()
	r.hook.take()

	return out
}

// The doors the round-2 re-check found unpinned (R2-2), and the two it found vacuous,
// re-done against a catalog that checks requirements so the retry and the validators
// really run.
func TestTheRemainingCommitDoorsReadThroughTheHook(t *testing.T) {
	ctx := context.Background()
	t.Run("root manifest mode, inline parent entries", func(t *testing.T) {
		r := newCoverageRig(t, iceberg.Properties{RootManifestKey: "inline"})
		r.appendOne(t)
		r.raw.take()
		r.hook.take()
		r.appendOne(t)
		requireEveryManifestOpenHooked(t, r, "root mode")
	})
	t.Run("AddDataFiles", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		dfs := r.dataFilesFor(t, r.next())
		r.raw.take()
		r.hook.take()
		txn := r.tbl.NewTransaction()
		require.NoError(t, txn.AddDataFiles(ctx, dfs, nil))
		landed, err := txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "AddDataFiles")
	})
	t.Run("ReplaceDataFilesWithDataFiles", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		old := r.currentDataFiles(t)
		add := r.dataFilesFor(t, r.next())
		r.raw.take()
		r.hook.take()
		txn := r.tbl.NewTransaction()
		require.NoError(t, txn.ReplaceDataFilesWithDataFiles(ctx, old, add, nil))
		landed, err := txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "ReplaceDataFilesWithDataFiles")
	})
	t.Run("ReplaceFiles", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		old := r.currentDataFiles(t)
		add := r.dataFilesFor(t, r.next())
		r.raw.take()
		r.hook.take()
		txn := r.tbl.NewTransaction()
		require.NoError(t, txn.ReplaceFiles(ctx, old, add, nil, nil))
		landed, err := txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "ReplaceFiles")
	})
	t.Run("Delete with a filter", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		r.raw.take()
		r.hook.take()
		txn := r.tbl.NewTransaction()
		require.NoError(t, txn.Delete(ctx, iceberg.AlwaysTrue{}, nil))
		landed, err := txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "Delete")
	})
	t.Run("ReplaceFiles with delete files to remove", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		posDel, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			r.tbl.Location()+"/pos-del-1.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		deleteFile := posDel.Build()
		rdTxn := r.tbl.NewTransaction()
		require.NoError(t, rdTxn.NewRowDelta(nil).AddDeletes(deleteFile).Commit(ctx))
		landed, err := rdTxn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		var old []iceberg.DataFile
		for _, df := range r.currentDataFiles(t) {
			if df.ContentType() == iceberg.EntryContentData {
				old = append(old, df)
			}
		}
		add := r.dataFilesFor(t, r.next())
		r.raw.take()
		r.hook.take()
		// A non-empty deleteFilesToRemove skips the delegation to
		// ReplaceDataFilesWithDataFiles and scans the snapshot at its own site.
		txn := r.tbl.NewTransaction()
		require.NoError(t, txn.ReplaceFiles(ctx, old, add, []iceberg.DataFile{deleteFile}, nil))
		landed, err = txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "ReplaceFiles with delete files")
	})
	t.Run("Delete with a real predicate", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		r.raw.take()
		r.hook.take()
		// AlwaysTrue short-circuits to a plain scan; a real predicate reaches the
		// filtered classification, which lists the manifests and opens each one.
		txn := r.tbl.NewTransaction()
		require.NoError(t, txn.Delete(ctx, iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(0)), nil))
		landed, err := txn.Commit(ctx)
		require.NoError(t, err)
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "filtered Delete")
	})
	t.Run("the rebuild on a real retry: the parent moved under the commit", func(t *testing.T) {
		r := newStrictRig(t, iceberg.Properties{CommitNumRetriesKey: "4", CommitMinRetryWaitMsKey: "1", CommitMaxRetryWaitMsKey: "2"})
		r.appendOne(t)
		stale := r.tbl
		r.appendOne(t) // a concurrent commit moves main
		r.raw.take()
		r.hook.take()
		txn := stale.NewTransaction()
		require.NoError(t, txn.AddFiles(ctx, []string{r.next()}, nil, false))
		landed, err := txn.Commit(ctx) // attempt 0 fails the ref requirement, the rebuild reads the moved parent
		require.NoError(t, err)
		require.GreaterOrEqual(t, int(r.cat.attempts.Load()), 4, "no retry happened")
		r.tbl = landed
		requireEveryManifestOpenHooked(t, r, "real retry rebuild")
	})
	t.Run("conflict validation on a real retry", func(t *testing.T) {
		r := newStrictRig(t, iceberg.Properties{CommitNumRetriesKey: "4", CommitMinRetryWaitMsKey: "1", CommitMaxRetryWaitMsKey: "2"})
		r.appendOne(t)
		stale := r.tbl
		old := fmt.Sprintf("%s/data-%d.parquet", r.tbl.Location(), r.n)
		r.appendOne(t) // the concurrent snapshot the validator walks
		r.raw.take()
		r.hook.take()
		txn := stale.NewTransaction()
		require.NoError(t, txn.ReplaceDataFiles(ctx, []string{old}, []string{r.next()}, nil))
		_, err := txn.Commit(ctx) // attempt 0 fails the ref requirement; attempt 1 runs the overwrite validator over the concurrent snapshot
		require.Error(t, err, "the overwrite from a stale base must be refused by its validator (concurrent data files added)")
		require.GreaterOrEqual(t, int(r.cat.attempts.Load()), 3, "no retry happened")
		requireEveryManifestOpenHooked(t, r, "real conflict validation")
	})
}

// The expire cleanup removes only under the table root (R2-1): a foreign data file a
// manifest named is left where it is, logged, while the expired snapshot's own
// manifest list is removed and the file the survivor still references stays.
func TestTheExpireCleanupRemovesOnlyUnderTheTableRoot(t *testing.T) {
	ctx := context.Background()
	r := newCoverageRig(t, nil)
	foreignDir := filepath.ToSlash(t.TempDir())
	foreign := foreignDir + "/foreign.parquet"
	r.write(foreign)
	txn := r.tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(ctx, []string{foreign}, nil, false)) // s1: a foreign data file inside an in-table manifest
	s1, err := txn.Commit(ctx)
	require.NoError(t, err)
	r.tbl = s1
	s1List := s1.CurrentSnapshot().ManifestList
	inTable := r.next()
	txn = r.tbl.NewTransaction()
	require.NoError(t, txn.ReplaceDataFiles(ctx, []string{foreign}, []string{inTable}, nil)) // s2 removes it
	s2, err := txn.Commit(ctx)
	require.NoError(t, err)
	r.tbl = s2
	txn = r.tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(ctx, []string{r.next()}, nil, false)) // s3, so s1 and s2 can go
	s3, err := txn.Commit(ctx)
	require.NoError(t, err)
	r.tbl = s3
	time.Sleep(5 * time.Millisecond)
	txn = r.tbl.NewTransaction()
	require.NoError(t, txn.ExpireSnapshots(WithRetainLast(1), WithOlderThan(0), WithPostCommit(true)))
	_, err = txn.Commit(ctx)
	require.NoError(t, err)
	exists := func(p string) bool { _, err := r.raw.Open(p); return err == nil }
	require.True(t, exists(foreign), "the expire cleanup removed a data file outside the table root")
	require.False(t, exists(s1List), "the expired snapshot's own manifest list was not removed")
	require.True(t, exists(inTable), "a data file the surviving snapshot still references was removed")
}

// R3-2 of the iceberg-go#17 round 3 re-check (caver-go#9565): the expire cleanup
// anchors its removes on the metadata path the table was LOADED from, not on the
// metadata's parsed location field. The three snapshots are committed honestly, so
// the loaded path stays under the table; then the cleanup is run directly, as
// doCommit would run it, on a pre-table whose location field has been moved to the
// common parent. A manifest names a sibling table's file under that parent: it must
// survive, a file under the loaded table still goes, and the surviving snapshot's
// file stays. Anchoring on Metadata().Location() removes the sibling's file and
// fails this test. (Committing AFTER moving the field would place the next metadata
// file under the moved location and drag the loaded path with it, which is the gap
// caver-go#9536 closes separately; the cleanup is probed on its own here so the
// anchor is what is measured.)
func TestTheExpireCleanupAnchorsOnTheLoadedPathNotTheMovedLocation(t *testing.T) {
	ctx := context.Background()
	r := newCoverageRig(t, nil)
	tableDir := strings.TrimSuffix(r.tbl.Location(), "/")
	parent := tableDir[:strings.LastIndexByte(tableDir, '/')]
	siblingFile := parent + "/sibling/data/victim.parquet"
	ownFile := tableDir + "/data/own-old.parquet"
	r.write(siblingFile)
	r.write(ownFile)

	txn := r.tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(ctx, []string{siblingFile, ownFile}, nil, false)) // s1: a sibling's file next to the table's own
	s1, err := txn.Commit(ctx)
	require.NoError(t, err)
	r.tbl = s1
	keep := r.next()
	txn = r.tbl.NewTransaction()
	require.NoError(t, txn.ReplaceDataFiles(ctx, []string{siblingFile, ownFile}, []string{keep}, nil)) // s2 drops both
	s2, err := txn.Commit(ctx)
	require.NoError(t, err)
	r.tbl = s2
	txn = r.tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(ctx, []string{r.next()}, nil, false)) // s3, so s1 and s2 can go
	s3, err := txn.Commit(ctx)
	require.NoError(t, err)
	r.tbl = s3
	require.Equal(t, tableDir+"/metadata", s3.metadataLocation[:strings.LastIndexByte(s3.metadataLocation, '/')], "premise: the loaded path is under the table")
	time.Sleep(5 * time.Millisecond)

	// The expire in metadata only: post is the table after s1 and s2 are gone from
	// the log, with nothing removed from storage yet.
	txn = r.tbl.NewTransaction()
	require.NoError(t, txn.ExpireSnapshots(WithRetainLast(1), WithOlderThan(0), WithPostCommit(false)))
	post, err := txn.Commit(ctx)
	require.NoError(t, err)
	require.Nil(t, post.Metadata().SnapshotByID(s1.CurrentSnapshot().SnapshotID), "premise: s1 expired")
	require.Nil(t, post.Metadata().SnapshotByID(s2.CurrentSnapshot().SnapshotID), "premise: s2 expired")

	// pre: s3's metadata with its location field moved to the common parent, still
	// loaded from the path under the table.
	bldr, err := MetadataBuilderFromBase(s3.Metadata(), "")
	require.NoError(t, err)
	require.NoError(t, bldr.SetLoc(parent))
	movedMeta, err := bldr.Build()
	require.NoError(t, err)
	require.Equal(t, parent, strings.TrimSuffix(movedMeta.Location(), "/"))
	pre := New(Identifier{"default", "cov"}, movedMeta, s3.metadataLocation, r.tbl.fsF, r.cat).
		WithManifestReadIO(func(fs iceio.IO) iceio.IO { r.hook.inner = fs; return r.hook })

	upd := NewRemoveSnapshotsUpdate([]int64{s1.CurrentSnapshot().SnapshotID, s2.CurrentSnapshot().SnapshotID}, true)
	require.NoError(t, upd.PostCommit(ctx, pre, post))

	exists := func(p string) bool { _, err := r.raw.Open(p); return err == nil }
	require.True(t, exists(siblingFile), "the expire cleanup followed a moved location field and removed a sibling table's file")
	require.False(t, exists(ownFile), "a file under the loaded table was not removed, so the cleanup ran nothing")
	require.True(t, exists(keep), "a file the surviving snapshot references was removed")
}

// The orphan scan builds its referenced set from every snapshot's list and manifests
// through the hook, and a list or manifest the hook refuses aborts the scan with
// nothing deleted (caver-go#9638). The scan runs on a table loaded from the catalog,
// as every real sweep does. The refusal names manifests by their "-m<N>.avro" shape:
// the first shape of this test refused "-m0.avro", a name no manifest this rig's
// appends write carries (they write "-m1.avro"), so the hook refused nothing and the
// scan returned nil; measured, not guessed, before the pattern was widened.
func TestTheOrphanScanReadsManifestsThroughTheHook(t *testing.T) {
	ctx := context.Background()
	scanTable := func(t *testing.T, r *coverageRig) *Table {
		t.Helper()
		loaded, err := r.cat.LoadTable(ctx, r.tbl.Identifier())
		require.NoError(t, err)

		return loaded.WithManifestReadIO(func(fs iceio.IO) iceio.IO { r.hook.inner = fs; return r.hook })
	}
	t.Run("every list and manifest open is hooked", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		r.appendOne(t)
		tbl := scanTable(t, r)
		r.raw.take()
		r.hook.take()
		_, err := tbl.DeleteOrphanFiles(ctx, WithDryRun(true), WithFilesOlderThan(0))
		require.NoError(t, err)
		requireEveryManifestOpenHooked(t, r, "orphan scan")
	})
	// stray is a parquet file under the table that no manifest references: the one
	// thing an unrefused scan deletes, so "deleted nothing" is a claim that can fail.
	stray := func(t *testing.T, r *coverageRig) string {
		t.Helper()
		p := strings.TrimSuffix(r.tbl.Location(), "/") + "/data/stray-orphan.parquet"
		r.write(p)
		past := time.Now().Add(-time.Hour)
		require.NoError(t, os.Chtimes(strings.TrimPrefix(p, "file://"), past, past))

		return p
	}
	t.Run("an unrefused scan deletes the stray (the premise of the next case)", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		r.appendOne(t)
		s := stray(t, r)
		tbl := scanTable(t, r)
		var deleted []string
		_, err := tbl.DeleteOrphanFiles(ctx, WithFilesOlderThan(0),
			WithDeleteFunc(func(p string) error { deleted = append(deleted, p); return nil }))
		require.NoError(t, err)
		require.Contains(t, deleted, s, "the stray was not listed, so the refusal case below could not have deleted it either")
	})
	t.Run("a refused manifest aborts the scan with nothing deleted", func(t *testing.T) {
		r := newCoverageRig(t, nil)
		r.appendOne(t)
		r.appendOne(t)
		s := stray(t, r)
		tbl := scanTable(t, r)
		r.hook.refuse = "-m" // every manifest file name is <commit uuid>-m<N>.avro; a uuid has no "-m"
		var deleted []string
		_, err := tbl.DeleteOrphanFiles(ctx, WithFilesOlderThan(0),
			WithDeleteFunc(func(p string) error { deleted = append(deleted, p); return nil }))
		require.ErrorIs(t, err, errHookRefused)
		require.Empty(t, deleted, "a refused manifest read must delete nothing, and there was a stray to delete")
		if _, err := r.raw.Open(s); err != nil {
			t.Fatalf("the stray is gone after a refused scan: %v", err)
		}
	})
}
