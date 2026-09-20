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

package io

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRelativePathHelpers(t *testing.T) {
	for p, want := range map[string]bool{
		"data/a.parquet":              true,
		"events_warm/metadata/m.avro": true,
		"/w/data/a.parquet":           false,
		"file:///w/data/a.parquet":    false,
		"s3://b/w/data/a.parquet":     false,
		"file:/w/data/a.parquet":      false,
		"C:/w/data/a.parquet":         false,
		"":                            false,
	} {
		if got := IsRelativePath(p); got != want {
			t.Errorf("IsRelativePath(%q) = %v, want %v", p, got, want)
		}
	}
	if got := JoinBase("s3://b/w/", "t/data/a.parquet"); got != "s3://b/w/t/data/a.parquet" {
		t.Errorf("JoinBase = %q", got)
	}
	if got := JoinBase("s3://b/w", "s3://other/x"); got != "s3://other/x" {
		t.Errorf("absolute must pass through: %q", got)
	}
	if got := JoinBase("", "t/data/a.parquet"); got != "t/data/a.parquet" {
		t.Errorf("empty base must pass through: %q", got)
	}
	if got := RelativizePath("s3://b/w", "s3://b/w/t/data/a.parquet"); got != "t/data/a.parquet" {
		t.Errorf("RelativizePath = %q", got)
	}
	if got := RelativizePath("s3://b/w", "s3://other/t/x"); got != "s3://other/t/x" {
		t.Errorf("a path under another root must stay absolute: %q", got)
	}
	// A local warehouse is spelled file:///w by the location provider and /w by the
	// local FileIO; both strip.
	if got := RelativizePath("file:///w", "/w/t/data/a.parquet"); got != "t/data/a.parquet" {
		t.Errorf("scheme-less local path under a file:// base: %q", got)
	}
	if got := RelativizePath("file:///w", "file:///w/t/data/a.parquet"); got != "t/data/a.parquet" {
		t.Errorf("file:// path under a file:// base: %q", got)
	}
	if got := RelativizePath("", "s3://b/w/x"); got != "s3://b/w/x" {
		t.Errorf("empty base must not strip: %q", got)
	}
}

func TestRelativeFSResolvesAgainstBase(t *testing.T) {
	dir := t.TempDir()
	base := filepath.ToSlash(dir)
	if err := os.MkdirAll(filepath.Join(dir, "t", "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	var fsys IO = WrapRelative(LocalFS{}, base)
	if PathBaseOf(fsys) != base {
		t.Fatalf("PathBaseOf = %q", PathBaseOf(fsys))
	}
	if PathBaseOf(LocalFS{}) != "" {
		t.Fatal("a plain FileIO has no base")
	}
	if WrapRelative(LocalFS{}, "") != (LocalFS{}) {
		t.Fatal("an empty base must not wrap")
	}
	wf := fsys.(WriteFileIO)
	if err := wf.WriteFile("t/data/a.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "t", "data", "a.txt")); err != nil {
		t.Fatalf("relative write must land under the base: %v", err)
	}
	b, err := fsys.(ReadFileIO).ReadFile("t/data/a.txt")
	if err != nil || string(b) != "hello" {
		t.Fatalf("relative read: %q %v", b, err)
	}
	// Absolute names pass through untouched.
	if b, err := fsys.(ReadFileIO).ReadFile(filepath.Join(dir, "t", "data", "a.txt")); err != nil || string(b) != "hello" {
		t.Fatalf("absolute read: %q %v", b, err)
	}
	f, err := fsys.Open("t/data/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	// WalkDir joins its root; DeleteFiles joins every path.
	seen := 0
	if err := fsys.(ListableIO).WalkDir("t", func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			seen++
		}

		return err
	}); err != nil || seen != 1 {
		t.Fatalf("WalkDir seen=%d err=%v", seen, err)
	}
	deleted, err := fsys.(BulkRemovableIO).DeleteFiles(context.Background(), []string{"t/data/a.txt"})
	if err != nil || len(deleted) != 1 {
		t.Fatalf("DeleteFiles = %v %v", deleted, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "t", "data", "a.txt")); !os.IsNotExist(err) {
		t.Fatal("DeleteFiles must remove the joined path")
	}
	// Wrapping a wrapper re-bases rather than nesting.
	if r := WrapRelative(fsys, "/other").(*RelativeFS); r.Base != "/other" || r.Inner != (LocalFS{}) {
		t.Fatalf("re-wrap = %+v", r)
	}
}

func TestLoadFSWrapsWhenAsked(t *testing.T) {
	ctx := context.Background()
	dir := filepath.ToSlash(t.TempDir())
	fsys, err := LoadFS(ctx, map[string]string{"warehouse": "file://" + dir, RelativePathsKey: RelativePathsWarehouse}, "")
	if err != nil {
		t.Fatal(err)
	}
	if PathBaseOf(fsys) != "file://"+dir {
		t.Fatalf("LoadFS must wrap with the warehouse as base, got %q", PathBaseOf(fsys))
	}
	plain, err := LoadFS(ctx, map[string]string{"warehouse": "file://" + dir}, "")
	if err != nil {
		t.Fatal(err)
	}
	if PathBaseOf(plain) != "" {
		t.Fatal("without the property LoadFS must not wrap")
	}
	off, err := LoadFS(ctx, map[string]string{RelativePathsKey: RelativePathsWarehouse}, "file://"+dir)
	if err != nil {
		t.Fatal(err)
	}
	if PathBaseOf(off) != "" {
		t.Fatal("without a warehouse there is no base to resolve against")
	}
}
