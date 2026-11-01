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

	"github.com/apache/iceberg-go/table/internal"
)

// FileObserver sees every record batch written to one data file and may attach
// parquet footer key/value metadata to it (caver-go#10675). See internal.FileObserver.
type FileObserver = internal.FileObserver

// FileObserverFactory returns the observer for one data file, or nil for none.
type FileObserverFactory = internal.FileObserverFactory

// WithFileObserver returns a context whose data-file writes (append, overwrite,
// compaction rewrite) are observed by f. A write under a context without one is
// unchanged.
func WithFileObserver(ctx context.Context, f FileObserverFactory) context.Context {
	return internal.WithFileObserver(ctx, f)
}
