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
	"io/fs"
	"strings"
)

// Relative paths (Iceberg v4 relative-paths proposal, ratified 2026-05; caver-go#4441).
//
// Every path a table's metadata stores today is an absolute URI, so relocating a
// warehouse means rewriting every metadata tree. With relative paths on, manifests and
// manifest lists store paths relative to a BASE (the warehouse root, so one table may
// reference files under a sibling table's prefix, which caver's metadata-only roll
// does), and the FileIO joins them back on every open. Nothing else in the library
// sees a relative string: readers join at decode time, writers strip at encode time,
// and an absolute path stays valid on read forever so mixed trees are fine.
//
// The base travels with the FileIO: LoadFS wraps the concrete IO in a RelativeFS when
// the properties carry RelativePathsKey and a warehouse. Writers ask the FileIO for
// its base through PathBaser; a FileIO without one makes writers emit absolute paths
// regardless of the table property, which is the fail-safe direction.

// RelativePathsKey is the property (table AND catalog/FileIO properties) that turns
// relative paths on. Its only recognised value is RelativePathsWarehouse.
const RelativePathsKey = "write.paths.relative"

// RelativePathsWarehouse makes stored paths relative to the warehouse root given by
// the "warehouse" property.
const RelativePathsWarehouse = "warehouse"

// PathBaser is implemented by a FileIO whose relative paths resolve against a base.
type PathBaser interface {
	PathBase() string
}

// PathBaseOf returns the base a FileIO joins relative paths against, or "" when the
// FileIO resolves nothing (absolute paths only).
func PathBaseOf(fsys any) string {
	if pb, ok := fsys.(PathBaser); ok {
		return pb.PathBase()
	}

	return ""
}

// IsRelativePath reports whether p is a relative path: no scheme and no leading
// slash. "file:///w/x", "file:/w/x" (path.Clean's spelling), "s3://b/x", "C:/x" and
// "/w/x" are absolute.
func IsRelativePath(p string) bool {
	if p == "" || strings.HasPrefix(p, "/") {
		return false
	}
	// A scheme (or a Windows drive letter) is anything before the first slash that
	// ends in a colon: "s3:", "file:", "C:".
	head := p
	if i := strings.IndexByte(p, '/'); i >= 0 {
		head = p[:i]
	}

	return !strings.HasSuffix(head, ":")
}

// JoinBase resolves p against base when p is relative; absolute paths and an empty
// base pass through unchanged.
func JoinBase(base, p string) string {
	if base == "" || !IsRelativePath(p) {
		return p
	}

	return strings.TrimRight(base, "/") + "/" + p
}

// RelativizePath strips base from p when p lies under it; anything else is returned
// unchanged (so a path under another root stays absolute rather than becoming wrong).
func RelativizePath(base, p string) string {
	if base == "" || p == "" {
		return p
	}
	prefix := strings.TrimRight(base, "/") + "/"
	if strings.HasPrefix(p, prefix) {
		return strings.TrimPrefix(p, prefix)
	}
	// A local warehouse is referenced both as file:///w and /w; accept either spelling.
	if alt := strings.TrimPrefix(prefix, "file://"); alt != prefix && strings.HasPrefix(p, alt) {
		return strings.TrimPrefix(p, alt)
	}

	return p
}

// RelativeFS wraps a FileIO so relative names resolve against Base. Every optional
// capability of the inner FileIO (ReadFile, Create, WriteFile, WalkDir, DeleteFiles)
// is forwarded when the inner FileIO has it.
type RelativeFS struct {
	Inner IO
	Base  string
}

// WrapRelative returns fsys resolving relative names against base; with an empty base
// it returns fsys unchanged.
func WrapRelative(fsys IO, base string) IO {
	if base == "" || fsys == nil {
		return fsys
	}
	if r, ok := fsys.(*RelativeFS); ok {
		return &RelativeFS{Inner: r.Inner, Base: base}
	}

	return &RelativeFS{Inner: fsys, Base: base}
}

func (r *RelativeFS) PathBase() string { return r.Base }

func (r *RelativeFS) join(name string) string { return JoinBase(r.Base, name) }

func (r *RelativeFS) Open(name string) (File, error) { return r.Inner.Open(r.join(name)) }

func (r *RelativeFS) Remove(name string) error { return r.Inner.Remove(r.join(name)) }

func (r *RelativeFS) ReadFile(name string) ([]byte, error) {
	if rf, ok := r.Inner.(ReadFileIO); ok {
		return rf.ReadFile(r.join(name))
	}
	f, err := r.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []byte
	buf := make([]byte, 32*1024)
	for {
		n, rerr := f.Read(buf)
		out = append(out, buf[:n]...)
		if rerr != nil {
			break
		}
	}

	return out, nil
}

func (r *RelativeFS) Create(name string) (FileWriter, error) {
	wf, ok := r.Inner.(WriteFileIO)
	if !ok {
		return nil, &fs.PathError{Op: "create", Path: name, Err: fs.ErrInvalid}
	}

	return wf.Create(r.join(name))
}

func (r *RelativeFS) WriteFile(name string, p []byte) error {
	wf, ok := r.Inner.(WriteFileIO)
	if !ok {
		return &fs.PathError{Op: "write", Path: name, Err: fs.ErrInvalid}
	}

	return wf.WriteFile(r.join(name), p)
}

func (r *RelativeFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	lf, ok := r.Inner.(ListableIO)
	if !ok {
		return &fs.PathError{Op: "walkdir", Path: root, Err: fs.ErrInvalid}
	}

	return lf.WalkDir(r.join(root), fn)
}

func (r *RelativeFS) DeleteFiles(ctx context.Context, paths []string) ([]string, error) {
	joined := make([]string, len(paths))
	for i, p := range paths {
		joined[i] = r.join(p)
	}
	if bf, ok := r.Inner.(BulkRemovableIO); ok {
		return bf.DeleteFiles(ctx, joined)
	}
	deleted := make([]string, 0, len(joined))
	for _, p := range joined {
		if err := r.Inner.Remove(p); err != nil {
			return deleted, err
		}
		deleted = append(deleted, p)
	}

	return deleted, nil
}

// relativeBaseFromProps returns the base a FileIO should resolve against, or "" when
// the properties do not ask for relative paths or name no warehouse.
func relativeBaseFromProps(props map[string]string) string {
	if props == nil || props[RelativePathsKey] != RelativePathsWarehouse {
		return ""
	}

	return strings.TrimRight(props["warehouse"], "/")
}
