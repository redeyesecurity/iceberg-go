# RedEye fork of apache/iceberg-go

Fork of `github.com/apache/iceberg-go`, branched off the upstream **v0.6.0** tag.
The module path is intentionally kept as `github.com/apache/iceberg-go` so caver-go
consumes it via a `replace` directive with no import churn, and so the patch stays a
minimal diff that rebases cleanly onto future upstream tags.

## Why

caver-go's lake writes Parquet through iceberg-go. Upstream v0.6.0 HARDCODES Parquet
dictionary encoding OFF in `GetWriteProperties`:

```
// table/internal/parquet_files.go (upstream v0.6.0)
parquet.WithDictionaryDefault(false)
```

There is no table property or config hook to turn it on. Dictionary encoding is a large
on-disk win for the low-cardinality columns the Caver lake is dominated by
(index / host / source / sourcetype), so we need it ON for new writes.

## The patch (single file: `table/internal/parquet_files.go`)

The change is the UPSTREAMABLE, property-honoring form: `GetWriteProperties` now reads a
table property `write.parquet.dictionary-enabled` (via `props.GetBool`, mirroring the
existing `props.GetInt` calls in the same writer-properties block). Default stays
`false`, so behavior is identical to upstream unless a table opts in. It is a pure
superset of upstream and could be sent upstream as-is.

1. New constants in the `write.parquet.*` const block:

   ```go
   ParquetDictEnabledKey     = "write.parquet.dictionary-enabled"
   ParquetDictEnabledDefault = false
   ```

2. `GetWriteProperties` honors it:

   ```go
   // was: parquet.WithDictionaryDefault(false)
   parquet.WithDictionaryDefault(props.GetBool(ParquetDictEnabledKey, ParquetDictEnabledDefault)),
   ```

Parquet auto-falls-back to PLAIN per column-chunk when a dictionary would exceed
`write.parquet.dict-size-bytes` (`WithDictionaryPageSizeLimit`), so enabling globally is
safe: high-cardinality columns silently stay PLAIN.

zstd compression, schema, partitioning, and the read path are untouched.

A unit test (`TestGetWritePropertiesDictionary` in
`table/internal/parquet_files_test.go`) asserts: absent property -> OFF (upstream
default), `"true"` -> ON, `"false"` -> OFF.

## How caver-go enables it

caver-go sets `write.parquet.dictionary-enabled = "true"` in the OCSF events-table write
properties at `internal/lake/iceberg.go` (`retentionTableProps`, applied at CreateTable
and ensured on open), and pins this fork via:

```
replace github.com/apache/iceberg-go => github.com/redeyesecurity/iceberg-go <tag/sha>
```

The property flows table-metadata -> `writeFiles(... meta.props ...)`
(`table/arrow_utils.go`) -> `GetWriteProperties(w.props)` (`table/writer.go`) at write
time, so only files written AFTER the table carries the property are dictionary-encoded.

## Re-syncing onto a future upstream release

The patch is two small hunks in one file on a branch based off `v0.6.0`. To move to a
newer upstream tag (e.g. `v0.7.0`):

```
git remote add upstream https://github.com/apache/iceberg-go      # once
git fetch upstream --tags
git checkout -b redeye-dict-encoding-v0.7.0 v0.7.0
git cherry-pick <this branch's patch commit>      # or re-apply the two hunks by hand
# resolve any drift in GetWriteProperties (rare), keep module path = apache/iceberg-go
go test ./table/internal/ -run TestGetWritePropertiesDictionary
git tag v0.7.0-redeye.1 && git push origin redeye-dict-encoding-v0.7.0 --tags
```

Then bump the `replace` line in caver-go's go.mod to the new tag/sha and `go mod tidy`.

If upstream adds a native `write.parquet.dictionary-enabled` (or equivalent) property,
drop the fork entirely and delete the `replace` line.
