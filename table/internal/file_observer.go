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
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// FileObserver sees every record batch written to ONE data file and may attach
// parquet footer key/value metadata to that file when it closes (caver-go#10675:
// a per-file index of the file's own rows, which lives and dies with the file).
//
// Observe is called with each batch before it is written. KeyValues is called once,
// after the last batch and before the footer is written; its pairs are appended to
// the footer in key order. An observer must not retain a batch past Observe.
type FileObserver interface {
	Observe(batch arrow.RecordBatch)
	KeyValues() map[string]string
}

// FileObserverFactory returns the observer for one data file, or nil for none. It is
// told the file's path, its content kind (data vs position/equality deletes) and its
// arrow schema, so it can decline files it has nothing to say about.
type FileObserverFactory func(path string, content iceberg.ManifestEntryContent, schema *arrow.Schema) FileObserver

type fileObserverKey struct{}

// WithFileObserver returns a context whose data-file writes are observed by f.
func WithFileObserver(ctx context.Context, f FileObserverFactory) context.Context {
	return context.WithValue(ctx, fileObserverKey{}, f)
}

func fileObserverFromContext(ctx context.Context, path string, content iceberg.ManifestEntryContent, schema *arrow.Schema) FileObserver {
	f, _ := ctx.Value(fileObserverKey{}).(FileObserverFactory)
	if f == nil {
		return nil
	}

	return f(path, content, schema)
}

// sortedKeyValues returns the observer's pairs in key order, so the footer bytes do
// not depend on map iteration order.
func sortedKeyValues(kv map[string]string) [][2]string {
	out := make([][2]string, 0, len(kv))
	for k, v := range kv {
		out = append(out, [2]string{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })

	return out
}
