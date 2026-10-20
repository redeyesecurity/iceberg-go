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
	"fmt"
	"strings"

	icebergio "github.com/apache/iceberg-go/io"
)

// ConfirmedLocationProvider is an optional CatalogIO interface: a catalog that
// records a table's location in its own store, where the metadata file's
// contents cannot reach it, hands it to the commit path and the orphan scan
// here (caver-go#9633, option A). The RedEye lake catalog keeps it on the
// signed pointer; the sql catalog keeps it in a column of iceberg_tables.
//
// ok=false means the catalog has no confirmed location for this table (a row
// or pointer written before the catalog recorded one) and the table keeps the
// behaviour it has without the interface. An error means the catalog could not
// be asked, and the commit or scan returns it unchanged: it is not a refusal.
type ConfirmedLocationProvider interface {
	ConfirmedTableLocation(ctx context.Context, ident Identifier) (loc string, ok bool, err error)
}

// ErrConfirmedLocationRefused is returned by a commit or an orphan scan whose
// table does not agree with the location its catalog confirmed. It wraps
// ErrMetadataWriteRefused, so a caller that already treats that refusal as
// permanent needs no new case.
var ErrConfirmedLocationRefused = fmt.Errorf("%w: catalog-confirmed location", ErrMetadataWriteRefused)

// CheckConfirmedLocation reports whether meta agrees with confirmed, the
// location the catalog vouches for: the location field names that directory,
// and the directories the table places files in (write.data.path or
// <location>/data, write.metadata.path or <location>/metadata) are under it.
// fsBase is the FileIO base relative paths resolve against; "" for none.
//
// This is the second source of truth the write anchor lacks. The anchor holds
// the next metadata file to the directory the pointer loaded from and accepts a
// split layout, so a plant that moves the location onto another table while
// pointing write.metadata.path home, or that sets write.data.path to another
// table's data directory, passes it. Against a confirmed location both are
// refused, and the legitimate split layout (both directories under the
// confirmed location) still commits.
func CheckConfirmedLocation(fsBase string, meta Metadata, confirmed string) error {
	if meta == nil || confirmed == "" {
		return nil
	}
	root := icebergio.JoinBase(fsBase, confirmed)
	loc := meta.Location()
	if loc == "" || !sameStorageDir(icebergio.JoinBase(fsBase, loc), root) {
		return fmt.Errorf("%w: the metadata names location %q, the catalog confirmed %q", ErrConfirmedLocationRefused, loc, confirmed)
	}
	provider, err := LoadLocationProvider(loc, meta.Properties())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrConfirmedLocationRefused, err)
	}
	for _, p := range []struct{ what, path string }{
		{"data", provider.NewDataLocation("f")},
		{"metadata", provider.NewMetadataLocation("f")},
	} {
		dir, ok := loadedMetadataDir(fsBase, p.path)
		if !ok || !storageDirUnder(dir, root) {
			return fmt.Errorf("%w: %s files are placed in %q, outside the confirmed location %q", ErrConfirmedLocationRefused, p.what, strings.TrimSuffix(p.path, "/f"), confirmed)
		}
	}

	return nil
}

// storageDirUnder reports whether dir is parent or a directory under it: same
// scheme and host, clean segments, and parent's path is a prefix of dir's at a
// separator.
func storageDirUnder(dir, parent string) bool {
	dScheme, dHost, dPath, ok := splitStoragePath(dir)
	if !ok {
		return false
	}
	pScheme, pHost, pPath, ok := splitStoragePath(parent)
	if !ok {
		return false
	}
	if !cleanSegments(dPath) || !cleanSegments(pPath) || dScheme != pScheme || dHost != pHost {
		return false
	}
	d, p := strings.TrimRight(dPath, "/"), strings.TrimRight(pPath, "/")

	return d == p || strings.HasPrefix(d, p+"/")
}

// confirmedLocation asks t's catalog for the location it vouches for.
func (t Table) confirmedLocation(ctx context.Context) (string, bool, error) {
	p, ok := t.cat.(ConfirmedLocationProvider)
	if !ok {
		return "", false, nil
	}

	return p.ConfirmedTableLocation(ctx, t.identifier)
}
