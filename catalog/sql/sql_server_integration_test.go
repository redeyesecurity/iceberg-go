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

//go:build integration

package sql_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The create path against a real SQL Server and a real Oracle (caver-go#9678).
// The sqlite suite cannot speak for either: SQL Server has its own string
// functions and range locks, and Oracle's serializable level is snapshot
// isolation, which is why its create takes a table lock instead (see
// withCreateTx). Each engine runs only when its URI is set, so the suite needs
// the servers, for example:
//
//	docker run -d -p 1433:1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Iceberg-2855 mcr.microsoft.com/mssql/server:2022-latest
//	docker run -d -p 1521:1521 -e ORACLE_PASSWORD=iceberg gvenzl/oracle-free:23-slim
//	ICEBERG_SQL_TEST_MSSQL_URI='sqlserver://sa:Iceberg-2855@localhost:1433?database=master' \
//	ICEBERG_SQL_TEST_ORACLE_URI='oracle://system:iceberg@localhost:1521/FREEPDB1' \
//	go test -tags integration -run TestSQLServerCatalogCreate ./catalog/sql/
//
// The SQL Server image does not start on arm64, so run it on an amd64 host.
func TestSQLServerCatalogCreate(t *testing.T) {
	for _, eng := range []struct {
		name    string
		env     string
		driver  string
		dialect sqlcat.SupportedDialect
	}{
		{"mssql", "ICEBERG_SQL_TEST_MSSQL_URI", "sqlserver", sqlcat.MSSQL},
		{"oracle", "ICEBERG_SQL_TEST_ORACLE_URI", "oracle", sqlcat.Oracle},
	} {
		t.Run(eng.name, func(t *testing.T) {
			uri := os.Getenv(eng.env)
			if uri == "" {
				t.Skipf("%s is not set", eng.env)
			}
			warehouse := t.TempDir()
			cat, err := catalog.Load(context.Background(), "server_"+eng.name, iceberg.Properties{
				"uri":             uri,
				sqlcat.DriverKey:  eng.driver,
				sqlcat.DialectKey: string(eng.dialect),
				"type":            "sql",
				"warehouse":       "file://" + warehouse,
			})
			require.NoError(t, err)
			sc := cat.(*sqlcat.Catalog)

			t.Run("create_and_refuse", func(t *testing.T) { serverCreateAndRefuse(t, sc, warehouse) })
			t.Run("concurrent_creates_land_once", func(t *testing.T) { serverConcurrentCreates(t, sc, warehouse, eng.dialect) })
			t.Run("held_creates_land_once", func(t *testing.T) { serverHeldCreates(t, sc, warehouse, eng.dialect) })
			t.Run("update_namespace_properties", func(t *testing.T) { serverUpdateNamespaceProperties(t, sc) })
		})
	}
}

func serverIdent(t *testing.T, warehouse string) table.Identifier {
	db, tbl := databaseName(), tableName()
	require.NoError(t, os.MkdirAll(filepath.Join(warehouse, db+".db", tbl, "metadata"), 0o755))

	return table.Identifier{db, tbl}
}

// The CreateTable path the sqlite suite covers, on the server: a create lands,
// a create onto another table's metadata directory is refused in both
// spellings and through a create by commit, and a directory that only shares a
// name prefix, or differs only in case, is free. Before caver-go#9666 the first
// create failed on SQL Server, which has no SUBSTR.
func serverCreateAndRefuse(t *testing.T, cat *sqlcat.Catalog, warehouse string) {
	ctx := context.Background()
	first := serverIdent(t, warehouse)
	ns := catalog.NamespaceFromIdent(first)
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	name := catalog.TableNameFromIdent(first)

	tbl, err := cat.CreateTable(ctx, first, tableSchemaNested)
	require.NoError(t, err, "a create on a free location")
	require.True(t, strings.HasPrefix(tbl.Location(), "file://"), "premise: the stored location carries the file:// scheme: %s", tbl.Location())

	second := table.Identifier{ns[0], "second_" + name}
	_, err = cat.CreateTable(ctx, second, tableSchemaNested, catalog.WithLocation(tbl.Location()))
	assert.ErrorIs(t, err, catalog.ErrTableAlreadyExists, "CreateTable onto the stored spelling")
	_, err = cat.CreateTable(ctx, second, tableSchemaNested, catalog.WithLocation(strings.TrimPrefix(tbl.Location(), "file://")))
	assert.ErrorIs(t, err, catalog.ErrTableAlreadyExists, "CreateTable onto the scheme-less spelling")

	third := table.Identifier{ns[0], "third_" + name}
	createByCommit := func(loc string) error {
		_, _, err := cat.CommitTable(ctx, third,
			[]table.Requirement{table.AssertCreate()},
			[]table.Update{
				table.NewAssignUUIDUpdate(uuid.New()),
				table.NewUpgradeFormatVersionUpdate(2),
				table.NewAddSchemaUpdate(tableSchemaNested),
				table.NewSetCurrentSchemaUpdate(-1),
				table.NewSetLocationUpdate(loc),
			})

		return err
	}
	assert.ErrorIs(t, createByCommit(tbl.Location()), catalog.ErrTableAlreadyExists, "create by commit onto another table's location")
	_, err = cat.LoadTable(ctx, third)
	assert.ErrorIs(t, err, catalog.ErrNoSuchTable, "the refused create by commit left no table behind")
	assert.NoError(t, createByCommit(tbl.Location()+"_third"), "control: a create by commit on a free location")

	sibling := table.Identifier{ns[0], "sibling_" + name}
	_, err = cat.CreateTable(ctx, sibling, tableSchemaNested, catalog.WithLocation(tbl.Location()+"2"))
	assert.NoError(t, err, "a location that only shares a name prefix is free")

	lower := table.Identifier{ns[0], "lower_" + name}
	_, err = cat.CreateTable(ctx, lower, tableSchemaNested, catalog.WithLocation("file://"+filepath.Join(warehouse, "case_"+name)))
	require.NoError(t, err)
	upper := table.Identifier{ns[0], "upper_" + name}
	_, err = cat.CreateTable(ctx, upper, tableSchemaNested, catalog.WithLocation("file://"+filepath.Join(warehouse, "CASE_"+name)))
	assert.NoError(t, err, "a location differing only in case is another directory")

	for _, id := range []table.Identifier{upper, lower, sibling, third, first} {
		if err := cat.DropTable(ctx, id); err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
			t.Errorf("drop %v: %v", id, err)
		}
	}
}

// UpdateNamespaceProperties overwrites, adds and removes in one call. Before
// caver-go#9682 it failed on Oracle, whose bun dialect claims ON CONFLICT.
func serverUpdateNamespaceProperties(t *testing.T, cat *sqlcat.Catalog) {
	ctx := context.Background()
	ns := table.Identifier{databaseName()}
	require.NoError(t, cat.CreateNamespace(ctx, ns, iceberg.Properties{"keep": "a", "change": "b", "drop": "c"}))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	summary, err := cat.UpdateNamespaceProperties(ctx, ns, []string{"drop"}, iceberg.Properties{"change": "B", "add": "d"})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"drop"}, summary.Removed)
	assert.ElementsMatch(t, []string{"change", "add"}, summary.Updated)

	props, err := cat.LoadNamespaceProperties(ctx, ns)
	require.NoError(t, err)
	assert.Equal(t, "a", props["keep"])
	assert.Equal(t, "B", props["change"], "an existing key is overwritten, not duplicated")
	assert.Equal(t, "d", props["add"])
	assert.NotContains(t, props, "drop")

	_, err = cat.UpdateNamespaceProperties(ctx, ns, nil, iceberg.Properties{"change": "C"})
	require.NoError(t, err, "a second update of the same key")
	props, err = cat.LoadNamespaceProperties(ctx, ns)
	require.NoError(t, err)
	assert.Equal(t, "C", props["change"])
}

// Concurrent creates onto one location land exactly once. On Oracle every loser
// waits on the create's table lock and is then refused by the check, so each
// loser's error is the refusal. On SQL Server a loser may instead be the
// deadlock victim of the serializable range locks, which the caller retries.
func serverConcurrentCreates(t *testing.T, cat *sqlcat.Catalog, warehouse string, dialect sqlcat.SupportedDialect) {
	ctx := context.Background()
	base := serverIdent(t, warehouse)
	ns := catalog.NamespaceFromIdent(base)
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	loc := "file://" + filepath.Join(warehouse, "shared_"+catalog.TableNameFromIdent(base))

	const n = 8
	errs := make([]error, n)
	idents := make([]table.Identifier, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		idents[i] = table.Identifier{ns[0], fmt.Sprintf("racer%d_%s", i, catalog.TableNameFromIdent(base))}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = cat.CreateTable(ctx, idents[i], tableSchemaNested, catalog.WithLocation(loc))
		}()
	}
	close(start)
	wg.Wait()

	landed := 0
	for i, err := range errs {
		if err == nil {
			landed++
			assert.NoError(t, cat.DropTable(ctx, idents[i]))

			continue
		}
		t.Logf("racer %d: %v", i, err)
		if dialect == sqlcat.Oracle {
			assert.ErrorIs(t, err, catalog.ErrTableAlreadyExists, "an Oracle loser is refused by the check, not by the database")
		}
	}
	assert.Equal(t, 1, landed, "concurrent creates onto one location: %v", errs)
}

// Two creates onto one location, the first held after its in-transaction check
// until the second also passes its check or three seconds go by. That is the
// interleaving the 8-way race above only reaches by luck: without the lock,
// Oracle's second create passes its check against the first's uncommitted row
// and both land. With it the second waits on the lock, so the first is released
// by the timeout, commits, and the second is refused by its check. On SQL Server
// both checks pass and the serializable range locks deadlock the two inserts,
// so one of them is the victim.
func serverHeldCreates(t *testing.T, cat *sqlcat.Catalog, warehouse string, dialect sqlcat.SupportedDialect) {
	ctx := context.Background()
	base := serverIdent(t, warehouse)
	ns := catalog.NamespaceFromIdent(base)
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	loc := "file://" + filepath.Join(warehouse, "held_"+catalog.TableNameFromIdent(base))

	var mu sync.Mutex
	checked := 0
	both := make(chan struct{})
	defer sqlcat.SetCreateCheckedHook(func(context.Context) {
		mu.Lock()
		checked++
		if checked == 2 {
			close(both)
		}
		mu.Unlock()
		select {
		case <-both:
		case <-time.After(3 * time.Second):
		}
	})()

	var errs [2]error
	idents := [2]table.Identifier{
		{ns[0], "held0_" + catalog.TableNameFromIdent(base)},
		{ns[0], "held1_" + catalog.TableNameFromIdent(base)},
	}
	var wg sync.WaitGroup
	for i := range idents {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = cat.CreateTable(ctx, idents[i], tableSchemaNested, catalog.WithLocation(loc))
		}()
	}
	wg.Wait()

	landed := 0
	for i, err := range errs {
		if err == nil {
			landed++
			assert.NoError(t, cat.DropTable(ctx, idents[i]))

			continue
		}
		t.Logf("held create %d: %v", i, err)
		if dialect == sqlcat.Oracle {
			assert.ErrorIs(t, err, catalog.ErrTableAlreadyExists, "the Oracle loser is refused by the check")
		}
	}
	t.Logf("creates past the in-transaction check: %d", checked)
	assert.Equal(t, 1, landed, "held creates onto one location: %v", errs)
}
