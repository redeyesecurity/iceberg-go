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
	"encoding/json"

	iceio "github.com/apache/iceberg-go/io"
)

// writeTableMetadataForTest mirrors catalog/internal.WriteTableMetadata (which this
// package cannot import) without compression: offload, then write the returned view.
func writeTableMetadataForTest(metadata Metadata, fs iceio.WriteFileIO, loc string) (err error) {
	metadata, err = OffloadSnapshots(metadata, fs, loc)
	if err != nil {
		return err
	}
	out, err := fs.Create(loc)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	return json.NewEncoder(out).Encode(metadata)
}
