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

import "context"

// Commit refusal observer (caver-go#9632).
//
// doCommit refuses a commit on a base that would place its next metadata file
// outside the directory it was loaded from (ErrMetadataWriteRefused,
// metadata_write_anchor.go) before Catalog.CommitTable runs, so the catalog
// never sees the commit it was refused on behalf of. A catalog that counts its
// own refusals would undercount: the in-process writer's refusal is returned
// to the caller and nowhere else.

// CommitRefusalObserver is an optional interface for a CatalogIO. When the
// table's catalog implements it, doCommit calls CommitRefused with the
// refusal before returning it, once per refused commit. The error is returned
// to the caller unchanged; the observer cannot turn the refusal into a commit.
type CommitRefusalObserver interface {
	CommitRefused(ctx context.Context, ident Identifier, err error)
}

// observeCommitRefusal tells cat about err when cat is a CommitRefusalObserver.
func observeCommitRefusal(ctx context.Context, cat CatalogIO, ident Identifier, err error) {
	if o, ok := cat.(CommitRefusalObserver); ok {
		o.CommitRefused(ctx, ident, err)
	}
}
