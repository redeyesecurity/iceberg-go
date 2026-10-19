package table_test

// manifest_read_io_test.go: a commit reads the parent snapshot's manifest list and
// manifests through the table's ManifestReadIO when one is installed, so a catalog
// that holds stored paths to a rule can refuse a list rewritten after its last check
// before the producer decodes it (caver-go#9520). Without the hook the commit reads
// through its own FileIO, as before.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// recordingIO is a FileIO that records every path opened through it and refuses the
// ones matching refuse, the shape of a containment check.
type recordingIO struct {
	iceio.IO
	mu     sync.Mutex
	opened []string
	refuse string
}

var errRefusedByRule = errors.New("refused by the path rule")

func (r *recordingIO) Open(name string) (iceio.File, error) {
	r.mu.Lock()
	r.opened = append(r.opened, name)
	r.mu.Unlock()
	if r.refuse != "" && strings.Contains(name, r.refuse) {
		return nil, fmt.Errorf("%w: %s", errRefusedByRule, name)
	}

	return r.IO.Open(name)
}

// hookCatalog is mergeCatalog with a LoadTable that answers, so Refresh works.
type hookCatalog struct {
	*mergeCatalog
	fsF table.FSysF
}

func (c *hookCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	return table.New(ident, c.meta, c.meta.Location()+"/metadata/current.json", c.fsF, c), nil
}

func newHookTestTable(t *testing.T) (*table.Table, iceio.LocalFS, func(path string)) {
	t.Helper()
	dir := t.TempDir()
	fs := iceio.LocalFS{}
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false}}, nil)
	writeParquet := func(path string) {
		bldr := array.NewInt32Builder(memory.DefaultAllocator)
		defer bldr.Release()
		bldr.AppendValues([]int32{1}, nil)
		col := bldr.NewArray()
		defer col.Release()
		rec := array.NewRecordBatch(arrowSchema, []arrow.Array{col}, 1)
		defer rec.Release()
		arrTbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
		defer arrTbl.Release()
		fo, err := fs.Create(path)
		require.NoError(t, err)
		require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(), nil, pqarrow.DefaultWriterProps()))
	}
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, dir, nil)
	require.NoError(t, err)
	fsF := func(_ context.Context) (iceio.IO, error) { return fs, nil }
	cat := &hookCatalog{mergeCatalog: &mergeCatalog{meta: meta}, fsF: fsF}
	tbl := table.New(table.Identifier{"default", "hook"}, meta, dir+"/metadata/00000.json", fsF, cat)

	return tbl, fs, writeParquet
}

// addOne appends one file and commits; the first error of the two steps is returned,
// since AddFiles reads the parent's manifests for its duplicate check before Commit.
func addOne(t *testing.T, tbl *table.Table, path string) (*table.Table, error) {
	t.Helper()
	txn := tbl.NewTransaction()
	if err := txn.AddFiles(context.Background(), []string{path}, nil, false); err != nil {
		return nil, err
	}

	return txn.Commit(context.Background())
}

// The second append's producer reads the parent's manifest list through the hook; a
// hook that refuses it fails the commit with its own error and nothing lands.
func TestACommitReadsTheParentManifestsThroughTheManifestReadIO(t *testing.T) {
	tbl, _, writeParquet := newHookTestTable(t)
	dir := tbl.Location()
	writeParquet(dir + "/data-0.parquet")
	parent, err := addOne(t, tbl, dir+"/data-0.parquet")
	require.NoError(t, err)
	parentList := parent.CurrentSnapshot().ManifestList
	require.NotEmpty(t, parentList)

	rec := &recordingIO{refuse: "/metadata/snap-"}
	hooked := parent.WithManifestReadIO(func(fs iceio.IO) iceio.IO { rec.IO = fs; return rec })
	writeParquet(dir + "/data-1.parquet")
	_, err = addOne(t, hooked, dir+"/data-1.parquet")
	require.ErrorIs(t, err, errRefusedByRule, "the commit read the parent's manifest list past the hook")
	require.Contains(t, rec.opened, parentList, "the hook was not asked for the parent's manifest list")
	require.NoError(t, hooked.Refresh(context.Background()))
	require.Equal(t, parent.CurrentSnapshot().SnapshotID, hooked.CurrentSnapshot().SnapshotID, "a refused commit moved the head")

	// Allowed by the hook: the commit lands and the hook saw the list and the manifest.
	rec.refuse = ""
	rec.opened = nil
	landed, err := addOne(t, hooked, dir+"/data-1.parquet")
	require.NoError(t, err)
	require.NotEqual(t, parent.CurrentSnapshot().SnapshotID, landed.CurrentSnapshot().SnapshotID)
	require.Contains(t, rec.opened, parentList)
}

// Without a hook the commit reads through its own FileIO, as it always did.
func TestACommitWithoutAManifestReadIOReadsItsOwnFileIO(t *testing.T) {
	tbl, _, writeParquet := newHookTestTable(t)
	dir := tbl.Location()
	writeParquet(dir + "/data-0.parquet")
	parent, err := addOne(t, tbl, dir+"/data-0.parquet")
	require.NoError(t, err)
	writeParquet(dir + "/data-1.parquet")
	landed, err := addOne(t, parent, dir+"/data-1.parquet")
	require.NoError(t, err)
	require.NotEqual(t, parent.CurrentSnapshot().SnapshotID, landed.CurrentSnapshot().SnapshotID)
}

// A Refresh keeps the hook: it is this process's reader, not the catalog's.
func TestRefreshKeepsTheManifestReadIO(t *testing.T) {
	tbl, _, writeParquet := newHookTestTable(t)
	dir := tbl.Location()
	writeParquet(dir + "/data-0.parquet")
	parent, err := addOne(t, tbl, dir+"/data-0.parquet")
	require.NoError(t, err)
	rec := &recordingIO{refuse: "/metadata/snap-"}
	hooked := parent.WithManifestReadIO(func(fs iceio.IO) iceio.IO { rec.IO = fs; return rec })
	require.NoError(t, hooked.Refresh(context.Background()))
	writeParquet(dir + "/data-1.parquet")
	_, err = addOne(t, hooked, dir+"/data-1.parquet")
	require.ErrorIs(t, err, errRefusedByRule, "the hook was lost on Refresh")
}
