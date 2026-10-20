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

// Package testhooks holds the seams the sql catalog's tests use to park a
// create inside its transaction. Both are nil in production. The package is
// internal to catalog/sql so only that package, its own tests, and the
// integration module under catalog/sql/integration can reach them; a separate
// module cannot see catalog/sql's export_test.go, which is why the hooks live
// here rather than in that file.
package testhooks

import "context"

// CreateTx, when a test sets it, runs inside a create's transaction before its
// held-directory check and insert.
var CreateTx func(context.Context)

// CreateChecked, when a test sets it, runs inside a create's transaction after
// its held-directory check and before its insert.
var CreateChecked func(context.Context)
