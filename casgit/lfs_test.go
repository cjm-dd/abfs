// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package casgit

import (
	"testing"

	"github.com/google/abfs/filesystem/dirsync"
	"github.com/google/abfs/shared/entities"
)

const testOID = "4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393"

func TestParseLFSPointer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		wantOID  string
		wantSize int64
		wantOK   bool
	}{
		{
			name:     "pointer",
			body:     "version https://git-lfs.github.com/spec/v1\noid sha256:" + testOID + "\nsize 12345\n",
			wantOID:  testOID,
			wantSize: 12345,
			wantOK:   true,
		},
		{
			name:     "extension lines",
			body:     "version https://git-lfs.github.com/spec/v1\next-0-foo sha256:" + testOID + "\noid sha256:" + testOID + "\nsize 0\n",
			wantOID:  testOID,
			wantSize: 0,
			wantOK:   true,
		},
		{
			name: "committed content",
			body: "not a pointer\n",
		},
		{
			name: "missing size",
			body: "version https://git-lfs.github.com/spec/v1\noid sha256:" + testOID + "\n",
		},
		{
			name: "short oid",
			body: "version https://git-lfs.github.com/spec/v1\noid sha256:abcd\nsize 1\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oid, size, ok := parseLFSPointer([]byte(tc.body))
			if ok != tc.wantOK || (ok && (oid != tc.wantOID || size != tc.wantSize)) {
				t.Errorf("parseLFSPointer() = %q, %d, %v; want %q, %d, %v", oid, size, ok, tc.wantOID, tc.wantSize, tc.wantOK)
			}
		})
	}
}

func TestDropHydratedLFS(t *testing.T) {
	pointer := entities.Hash{1}
	content := entities.Hash{2}
	edited := entities.Hash{3}
	state := lfsState{"model.bin": {Pointer: pointer, Content: content, Size: 10}}
	diff := []dirsync.DiffAction{
		{Path: "model.bin", HashA: pointer, HashB: content},
		{Path: "src/main.go", HashA: entities.Hash{4}, HashB: entities.Hash{5}},
	}

	got := dropHydratedLFS(append([]dirsync.DiffAction(nil), diff...), state)
	if len(got) != 1 || got[0].Path != "src/main.go" {
		t.Errorf("hydrated file not dropped: %+v", got)
	}

	diff[0].HashB = edited
	got = dropHydratedLFS(append([]dirsync.DiffAction(nil), diff...), state)
	if len(got) != 2 {
		t.Errorf("edited LFS file dropped: %+v", got)
	}
}
