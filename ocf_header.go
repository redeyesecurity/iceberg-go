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

package iceberg

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/twmb/avro/ocf"
)

// OCF header handling for manifests whose header carries more than the avro
// library will read (caver-go#4493).
//
// github.com/twmb/avro/ocf caps every header map value at 1 MiB. A root manifest
// (root_manifest.go) keeps its child list in the header, and a table with tens of
// thousands of manifests writes a child list several MiB long: valid avro that the
// library then refuses with "map value length N out of range". Manifests are read
// through newOCFReader instead: it parses the header itself, with no per-value cap,
// and hands the library a stream whose header omits any oversized caver.* key. The
// full header is returned alongside so the root reader still sees its children.
// Standard Iceberg keys are never dropped: an oversized standard key is a corrupt
// file and stays an error.

const (
	// ocfHeaderValueCap mirrors the avro library's per-value limit.
	ocfHeaderValueCap = 1 << 20
	// ocfHeaderMapCap bounds what this parser accepts for one value so a corrupt
	// length cannot ask for gigabytes.
	ocfHeaderMapCap = 1 << 30
	ocfSyncLen      = 16
)

var ocfMagic = []byte{'O', 'b', 'j', 1}

// ocfHeader is a parsed OCF header.
type ocfHeader struct {
	meta map[string][]byte
	sync [ocfSyncLen]byte
}

// readOCFHeader consumes an OCF header from r.
func readOCFHeader(r *bufio.Reader) (*ocfHeader, error) {
	magic := make([]byte, len(ocfMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("ocf: reading magic: %w", err)
	}
	if !bytes.Equal(magic, ocfMagic) {
		return nil, errors.New("ocf: invalid magic")
	}
	h := &ocfHeader{meta: map[string][]byte{}}
	for {
		count, err := binary.ReadVarint(r)
		if err != nil {
			return nil, fmt.Errorf("ocf: reading metadata: %w", err)
		}
		if count == 0 {
			break
		}
		if count < 0 {
			count = -count
			if _, err := binary.ReadVarint(r); err != nil {
				return nil, fmt.Errorf("ocf: reading metadata block size: %w", err)
			}
		}
		if count > 1<<20 {
			return nil, fmt.Errorf("ocf: metadata block count %d exceeds safety limit", count)
		}
		for range int(count) {
			key, err := readOCFBytes(r, ocfHeaderValueCap)
			if err != nil {
				return nil, fmt.Errorf("ocf: reading metadata key: %w", err)
			}
			val, err := readOCFBytes(r, ocfHeaderMapCap)
			if err != nil {
				return nil, fmt.Errorf("ocf: reading metadata value for %q: %w", key, err)
			}
			h.meta[string(key)] = val
		}
	}
	if _, err := io.ReadFull(r, h.sync[:]); err != nil {
		return nil, fmt.Errorf("ocf: reading sync marker: %w", err)
	}

	return h, nil
}

func readOCFBytes(r *bufio.Reader, limit int64) ([]byte, error) {
	n, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	if n < 0 || n > limit {
		return nil, fmt.Errorf("length %d out of range", n)
	}
	b := make([]byte, int(n))
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}

	return b, nil
}

// oversized reports the header keys whose values the avro library would refuse.
func (h *ocfHeader) oversized() []string {
	var keys []string
	for k, v := range h.meta {
		if len(v) > ocfHeaderValueCap {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	return keys
}

// encodeWithout re-encodes the header without the named keys, in canonical order.
func (h *ocfHeader) encodeWithout(drop map[string]bool) []byte {
	keys := make([]string, 0, len(h.meta))
	for k := range h.meta {
		if !drop[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := append([]byte{}, ocfMagic...)
	out = binary.AppendVarint(out, int64(len(keys)))
	for _, k := range keys {
		out = binary.AppendVarint(out, int64(len(k)))
		out = append(out, k...)
		out = binary.AppendVarint(out, int64(len(h.meta[k])))
		out = append(out, h.meta[k]...)
	}
	out = append(out, 0)
	out = append(out, h.sync[:]...)

	return out
}

// newOCFReader opens an avro OCF stream the way ocf.NewReader does, tolerating
// header values above the library's cap for keys in the caver namespace. The
// returned metadata is the complete header, oversized values included; the
// reader's own Metadata() omits them.
func newOCFReader(in io.Reader, opts ...ocf.ReaderOpt) (*ocf.Reader, map[string][]byte, error) {
	br := bufio.NewReaderSize(in, 64<<10)
	h, err := readOCFHeader(br)
	if err != nil {
		return nil, nil, err
	}
	var stream io.Reader = br
	if big := h.oversized(); len(big) > 0 {
		drop := make(map[string]bool, len(big))
		for _, k := range big {
			if !strings.HasPrefix(k, "caver.") {
				return nil, nil, fmt.Errorf("ocf: reading metadata: value for %q is %d bytes, over the %d byte limit", k, len(h.meta[k]), ocfHeaderValueCap)
			}
			drop[k] = true
		}
		stream = io.MultiReader(bytes.NewReader(h.encodeWithout(drop)), br)
	} else {
		stream = io.MultiReader(bytes.NewReader(h.encodeWithout(nil)), br)
	}
	rd, err := ocf.NewReader(stream, opts...)
	if err != nil {
		return nil, nil, err
	}

	return rd, h.meta, nil
}
