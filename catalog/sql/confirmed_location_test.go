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

package sql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/driver/sqliteshim"
)

// confirmedRig is a sqlite catalog on a file database with a local warehouse,
// so a second open of the same database is possible.
type confirmedRig struct {
	dsn, warehouse string
}

func newConfirmedRig(t *testing.T) confirmedRig {
	t.Helper()
	dir := t.TempDir()

	return confirmedRig{dsn: "file:" + filepath.Join(dir, "catalog.db"), warehouse: filepath.Join(dir, "wh")}
}

func (r confirmedRig) open(t *testing.T) *sqlcat.Catalog {
	t.Helper()
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":             r.dsn,
		sqlcat.DriverKey:  sqliteshim.ShimName,
		sqlcat.DialectKey: string(sqlcat.SQLite),
		"type":            "sql",
		"warehouse":       "file://" + r.warehouse,
	})
	require.NoError(t, err)

	return cat.(*sqlcat.Catalog)
}

func (r confirmedRig) exec(t *testing.T, q string) {
	t.Helper()
	db, err := sql.Open(sqliteshim.ShimName, r.dsn)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(q)
	require.NoError(t, err)
}

func createIn(t *testing.T, cat *sqlcat.Catalog, name string) (*table.Table, table.Identifier) {
	t.Helper()
	ctx := context.Background()
	ident := table.Identifier{"db", name}
	tbl, err := cat.CreateTable(ctx, ident, tableSchemaNested)
	require.NoError(t, err)

	return tbl, ident
}

// plantFile rewrites the table's current metadata.json in place: the location
// field to loc and props into the properties, with the catalog pointer unmoved.
func plantFile(t *testing.T, tbl *table.Table, loc string, props map[string]string) {
	t.Helper()
	p := strings.TrimPrefix(tbl.MetadataLocation(), "file://")
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	raw["location"] = loc
	pm, _ := raw["properties"].(map[string]any)
	if pm == nil {
		pm = map[string]any{}
	}
	for k, v := range props {
		pm[k] = v
	}
	raw["properties"] = pm
	b, err = json.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(p, b, 0o644))
}

func TestSqlCatalogConfirmsTheLocationItAssigned9633(t *testing.T) {
	ctx := context.Background()
	r := newConfirmedRig(t)
	cat := r.open(t)
	require.NoError(t, cat.CreateNamespace(ctx, table.Identifier{"db"}, nil))
	var _ table.ConfirmedLocationProvider = cat

	tbl, ident := createIn(t, cat, "t")
	loc, ok, err := cat.ConfirmedTableLocation(ctx, ident)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, tbl.Location(), loc)

	t.Run("unknown table confirms nothing", func(t *testing.T) {
		loc, ok, err := cat.ConfirmedTableLocation(ctx, table.Identifier{"db", "nope"})
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Empty(t, loc)
	})

	t.Run("a row with no location confirms nothing and still commits", func(t *testing.T) {
		r.exec(t, "UPDATE iceberg_tables SET location = NULL WHERE table_name = 't'")
		_, ok, err := cat.ConfirmedTableLocation(ctx, ident)
		require.NoError(t, err)
		assert.False(t, ok)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"k": "v"}))
		_, err = tx.Commit(ctx)
		require.NoError(t, err)
	})

	t.Run("a create-by-commit has no row to ask", func(t *testing.T) {
		staged, err := catalog.ToIdentifier("db", "staged"), error(nil)
		require.NoError(t, err)
		_, ok, err := cat.ConfirmedTableLocation(ctx, staged)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestSqlCatalogAddsTheLocationColumnToAnOlderDatabase9633(t *testing.T) {
	ctx := context.Background()
	r := newConfirmedRig(t)
	first := r.open(t)
	require.NoError(t, first.CreateNamespace(ctx, table.Identifier{"db"}, nil))
	_, oldIdent := createIn(t, first, "old")

	// The database as a catalog before this change left it: no column.
	r.exec(t, "ALTER TABLE iceberg_tables DROP COLUMN location")

	second := r.open(t) // the probe fails, the column is added
	loc, ok, err := second.ConfirmedTableLocation(ctx, oldIdent)
	require.NoError(t, err)
	assert.False(t, ok, "a row from before the column confirms nothing")
	assert.Empty(t, loc)

	old, err := second.LoadTable(ctx, oldIdent)
	require.NoError(t, err, "the older row still loads")
	tx := old.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{"k": "v"}))
	_, err = tx.Commit(ctx)
	require.NoError(t, err, "and still commits")

	fresh, freshIdent := createIn(t, second, "fresh")
	loc, ok, err = second.ConfirmedTableLocation(ctx, freshIdent)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, fresh.Location(), loc)

	third := r.open(t) // the probe passes, nothing to add
	_, ok, err = third.ConfirmedTableLocation(ctx, freshIdent)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestAddLocationColumnSQLPerDialect9633(t *testing.T) {
	assert.Equal(t, "ALTER TABLE iceberg_tables ADD location NVARCHAR(2048) NULL", sqlcat.AddLocationColumnSQL(dialect.MSSQL))
	assert.Equal(t, "ALTER TABLE iceberg_tables ADD location VARCHAR2(2048)", sqlcat.AddLocationColumnSQL(dialect.Oracle))
	for _, d := range []dialect.Name{dialect.SQLite, dialect.PG, dialect.MySQL} {
		assert.Equal(t, "ALTER TABLE iceberg_tables ADD COLUMN location VARCHAR(2048)", sqlcat.AddLocationColumnSQL(d), d.String())
	}
}

// The two plants the write anchor cannot see, through the sqlite catalog as
// the confirming catalog: refused in the fork before CommitTable.
func TestSqlCatalogRefusesThePlantsAgainstItsConfirmedLocation9633(t *testing.T) {
	ctx := context.Background()
	r := newConfirmedRig(t)
	cat := r.open(t)
	require.NoError(t, cat.CreateNamespace(ctx, table.Identifier{"db"}, nil))
	hot, _ := createIn(t, cat, "hot")
	hotHead := hot.MetadataLocation()

	t.Run("the split plant: location moved onto hot, write.metadata.path home", func(t *testing.T) {
		victim, ident := createIn(t, cat, "t1")
		plantFile(t, victim, hot.Location(), map[string]string{table.WriteMetadataPathKey: victim.Location() + "/metadata"})
		planted, err := cat.LoadTable(ctx, ident)
		require.NoError(t, err)
		require.Equal(t, hot.Location(), planted.Location(), "the plant took")

		tx := planted.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"k": "v"}))
		_, err = tx.Commit(ctx)
		require.ErrorIs(t, err, table.ErrConfirmedLocationRefused)

		again, err := cat.LoadTable(ctx, ident)
		require.NoError(t, err)
		assert.Equal(t, planted.MetadataLocation(), again.MetadataLocation(), "the head did not move")
		reloaded, err := cat.LoadTable(ctx, table.Identifier{"db", "hot"})
		require.NoError(t, err)
		assert.Equal(t, hotHead, reloaded.MetadataLocation(), "hot is untouched")
	})

	t.Run("residual 1: write.data.path on hot, location home", func(t *testing.T) {
		victim, ident := createIn(t, cat, "t2")
		plantFile(t, victim, victim.Location(), map[string]string{table.WriteDataPathKey: hot.Location() + "/data"})
		planted, err := cat.LoadTable(ctx, ident)
		require.NoError(t, err)
		tx := planted.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"k": "v"}))
		_, err = tx.Commit(ctx)
		require.ErrorIs(t, err, table.ErrConfirmedLocationRefused)
	})

	t.Run("an orphan scan named at hot is refused", func(t *testing.T) {
		tbl, _ := createIn(t, cat, "t3")
		_, err := tbl.DeleteOrphanFiles(ctx, table.WithDryRun(true), table.WithLocation(hot.Location()))
		require.ErrorIs(t, err, table.ErrConfirmedLocationRefused)
		_, err = tbl.DeleteOrphanFiles(ctx, table.WithDryRun(true))
		require.NoError(t, err, "its own root scans")
	})
}
