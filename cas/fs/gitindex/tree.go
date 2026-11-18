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

package gitindex

import (
	"context"
	"time"
)

// FileEntry represents a file, symlink, or submodule in the tree.
type FileEntry struct {
	Name string
	Sha  [20]byte
	Mode uint32
	Size uint32
	// ContentSha, when set, is the CASFS hash of a file whose working tree
	// content differs from its blob, such as a hydrated Git LFS pointer. A
	// file with this hash on disk is clean, and Size is the content's size.
	ContentSha [20]byte
}

// TreeDirectory represents a directory node in the initial state tree.
type TreeDirectory interface {
	Name() string
	Sha() [20]byte
	Files(ctx context.Context) ([]FileEntry, error)
	Subtrees(ctx context.Context) ([]TreeDirectory, error)
}

// maxRacyWait bounds AwaitNotRacy, so an mtime from a skewed clock cannot
// stall index creation.
const maxRacyWait = 2 * time.Second

// AwaitNotRacy blocks until the wall clock is past the newest mtime, in whole
// seconds, of the entries whose content differs from their blob, so an index
// written afterwards has none of them racily clean. Git built without
// USE_NSEC rehashes racily clean entries on the next command, which for a
// hydrated Git LFS file runs the clean filter over its content. A racily
// clean ordinary file costs git one rehash, as it would without ABFS.
func (r *Result) AwaitNotRacy() {
	deadline := time.Now().Add(maxRacyWait)
	for now := time.Now(); now.Unix() <= int64(r.newestContentMtime) && now.Before(deadline); now = time.Now() {
		time.Sleep(10 * time.Millisecond)
	}
}
