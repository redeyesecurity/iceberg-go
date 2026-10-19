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
	meta, err := UpdateTableMetadata(c.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.meta = meta

	return meta, "", nil
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
