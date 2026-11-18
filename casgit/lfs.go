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
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/abfs/binding"
	"github.com/google/abfs/filesystem/dirsync"
	"github.com/google/abfs/shared/entities"
	"github.com/google/abfs/shared/objects/gitobjects"
)

const (
	// lfsStateFile, in the git dir, records the hydrated Git LFS files of the
	// working tree.
	lfsStateFile = "abfs-lfs.json"
	// lfsPointerMaxSize is the largest pointer file git-lfs will parse.
	lfsPointerMaxSize = 1024
	lfsPointerVersion = "version https://git-lfs.github.com/spec/v1"
	lfsWorkers        = 32
	// lfsLookupWorkers bounds concurrent pointer reads and map lookups, which
	// wait on the server rather than the CPU.
	lfsLookupWorkers = 128
)

// lfsFile is a working tree file whose blob is a Git LFS pointer and whose
// CASFS hash is the pointed-to content.
type lfsFile struct {
	Pointer entities.Hash `json:"pointer"`
	Content entities.Hash `json:"content"`
	Size    int64         `json:"size"`
	// OID is the git-lfs oid, the SHA-256 of the content.
	OID string `json:"oid"`
}

// lfsState maps working tree paths to hydrated Git LFS files.
type lfsState map[string]lfsFile

func readLFSState(gitDir string) lfsState {
	data, err := os.ReadFile(filepath.Join(gitDir, lfsStateFile))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("cannot read %s: %v", lfsStateFile, err)
		}
		return nil
	}
	var s lfsState
	if err := json.Unmarshal(data, &s); err != nil {
		log.Printf("ignoring corrupt %s: %v", lfsStateFile, err)
		return nil
	}
	return s
}

func writeLFSState(gitDir string, s lfsState) error {
	p := filepath.Join(gitDir, lfsStateFile)
	if len(s) == 0 {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

// dropHydratedLFS removes the entries of a HEAD-to-working-tree diff that
// are hydrated Git LFS files rather than local changes.
func dropHydratedLFS(diff []dirsync.DiffAction, s lfsState) []dirsync.DiffAction {
	if len(s) == 0 {
		return diff
	}
	kept := diff[:0]
	for _, d := range diff {
		if f, ok := s[d.Path]; ok && d.HashA == f.Pointer && d.HashB == f.Content {
			continue
		}
		kept = append(kept, d)
	}
	return kept
}

type treeFile struct {
	path string
	hash entities.Hash
	size int64
}

// regularFiles lists the regular files of a deep tree.
func (g *CASGit) regularFiles(ctx context.Context, entries []*entities.DeepEntry, prefix string, out *[]treeFile) error {
	for i := 1; i < len(entries); {
		ent := entries[i]
		p := path.Join(prefix, ent.Name)
		switch ent.Mode {
		case entities.FileModeDir:
			span := int(ent.Size)
			if err := g.regularFiles(ctx, entries[i:i+span], p, out); err != nil {
				return err
			}
			i += span
		case entities.FileModeDeepTree:
			dt, err := binding.FetchObject[gitobjects.DeepTreeObject](ctx, g.store, ent.Hash)
			if err != nil {
				return err
			}
			if err := g.regularFiles(ctx, dt.Entries, p, out); err != nil {
				return err
			}
			i++
		case entities.FileModeRegular, entities.FileModeExecutable:
			*out = append(*out, treeFile{path: p, hash: ent.Hash, size: ent.Size})
			i++
		default:
			i++
		}
	}
	return nil
}

// lfsFilterPaths returns the paths whose filter attribute is lfs, as the
// .gitattributes files in workDir set it.
func lfsFilterPaths(workDir, gitDir string, files []treeFile) (map[string]bool, error) {
	absGitDir, err := filepath.Abs(gitDir)
	if err != nil {
		return nil, err
	}
	var in bytes.Buffer
	for _, f := range files {
		in.WriteString(f.path)
		in.WriteByte(0)
	}
	cmd := exec.Command("git", "check-attr", "--stdin", "-z", "filter")
	cmd.Dir = workDir
	// git falls back to the index for .gitattributes files missing from the
	// working tree. During a checkout the index is still the old tree's, so a
	// .gitattributes the checkout deleted would still apply; an index path
	// that does not exist reads as empty.
	cmd.Env = append(os.Environ(),
		"GIT_DIR="+absGitDir,
		"GIT_WORK_TREE="+workDir,
		"GIT_INDEX_FILE="+filepath.Join(absGitDir, "abfs-lfs-no-index"))
	cmd.Stdin = &in
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git check-attr: %w: %s", err, stderr.String())
	}
	fields := bytes.Split(out, []byte{0})
	lfs := make(map[string]bool)
	for i := 0; i+2 < len(fields); i += 3 {
		if string(fields[i+2]) == "lfs" {
			lfs[string(fields[i])] = true
		}
	}
	return lfs, nil
}

// readBlob reads up to limit bytes of a git blob, fetching it from the
// server if the local cache lacks it.
func (g *CASGit) readBlob(ctx context.Context, h entities.Hash, limit int64) (_ []byte, retErr error) {
	r, err := g.store.OpenBlob(ctx, h)
	if err != nil {
		return nil, err
	}
	defer entities.CloseErrCheck(r, &retErr)
	return io.ReadAll(io.LimitReader(r, limit))
}

// parseLFSPointer returns the sha256 oid and size of a Git LFS pointer.
func parseLFSPointer(body []byte) (oid string, size int64, ok bool) {
	sc := bufio.NewScanner(bytes.NewReader(body))
	if !sc.Scan() || sc.Text() != lfsPointerVersion {
		return "", 0, false
	}
	size = -1
	for sc.Scan() {
		key, val, _ := strings.Cut(sc.Text(), " ")
		switch key {
		case "oid":
			oid, _ = strings.CutPrefix(val, "sha256:")
		case "size":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return "", 0, false
			}
			size = n
		}
	}
	return oid, size, len(oid) == 64 && size >= 0
}

// lfsContent resolves an LFS oid to its ABFS content hash through the map
// git-pusher records on the server.
func (g *CASGit) lfsContent(ctx context.Context, oid string) (entities.Hash, error) {
	req := &binding.LFSRequest{LFSOID: oid}
	if err := req.Encode(); err != nil {
		return entities.Hash{}, err
	}
	// LFS results are content-addressed and never change, so a cached result
	// is current; ask the server only on a miss, as abfs mount does.
	ids, err := g.store.MapResults(ctx, req.Hash)
	if err != nil {
		return entities.Hash{}, err
	}
	if len(ids) == 0 {
		if err := g.store.RefreshMapResults(ctx, req.Hash); err != nil {
			return entities.Hash{}, err
		}
		if ids, err = g.store.MapResults(ctx, req.Hash); err != nil {
			return entities.Hash{}, err
		}
	}
	if len(ids) != 1 {
		return entities.Hash{}, fmt.Errorf("%d LFS results", len(ids))
	}
	res, err := binding.FetchObject[binding.LFSResult](ctx, g.store, ids[0])
	if err != nil {
		return entities.Hash{}, err
	}
	return res.ABFSOID, nil
}

// HydrateLFS points each Git LFS pointer file in workDir at its content,
// when the file is still the pointer from deepTree and the server has the
// content, and records the result for index writes and local-change checks.
// Files with other content are left alone, so local edits survive. Setting
// GIT_LFS_SKIP_SMUDGE=1 leaves pointers in place, as it does for git-lfs.
func (g *CASGit) HydrateLFS(ctx context.Context, workDir, gitDir string, deepTree *gitobjects.DeepTreeObject) error {
	if os.Getenv("GIT_LFS_SKIP_SMUDGE") == "1" {
		return writeLFSState(gitDir, nil)
	}

	var files []treeFile
	if err := g.regularFiles(ctx, deepTree.Entries, "", &files); err != nil {
		return err
	}
	small := files[:0]
	for _, f := range files {
		if f.size < lfsPointerMaxSize {
			small = append(small, f)
		}
	}
	lfsPaths, err := lfsFilterPaths(workDir, gitDir, small)
	if err != nil {
		return err
	}
	var pointers []treeFile
	for _, f := range small {
		if lfsPaths[f.path] {
			pointers = append(pointers, f)
		}
	}
	if len(pointers) == 0 {
		return writeLFSState(gitDir, nil)
	}

	known := make(map[entities.Hash]lfsFile)
	for _, f := range readLFSState(gitDir) {
		if f.OID != "" { // Written before oids were recorded.
			known[f.Pointer] = f
		}
	}

	// Reuse what the previous hydration resolved, then read each remaining
	// distinct pointer blob and resolve it.
	resolved := make(map[entities.Hash]lfsFile)
	seen := make(map[entities.Hash]bool)
	var todo []treeFile
	for _, p := range pointers {
		if seen[p.hash] {
			continue
		}
		seen[p.hash] = true
		if f, ok := known[p.hash]; ok {
			resolved[p.hash] = f
		} else {
			todo = append(todo, p)
		}
	}

	var mu sync.Mutex
	unresolved := 0
	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(lfsLookupWorkers)
	for _, p := range todo {
		grp.Go(func() error {
			body, err := g.readBlob(gctx, p.hash, lfsPointerMaxSize)
			if err != nil {
				return fmt.Errorf("cannot read LFS pointer %s: %w", p.path, err)
			}
			oid, size, ok := parseLFSPointer(body)
			if !ok {
				return nil // Committed as content, not as a pointer.
			}
			content, err := g.lfsContent(gctx, oid)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				unresolved++
				return nil
			}
			resolved[p.hash] = lfsFile{Pointer: p.hash, Content: content, Size: size, OID: oid}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	if unresolved > 0 {
		log.Printf("%d Git LFS objects have no content on the ABFS server; their files stay as pointers", unresolved)
	}

	state := make(lfsState)
	grp, _ = errgroup.WithContext(ctx)
	grp.SetLimit(lfsWorkers)
	for _, p := range pointers {
		f, ok := resolved[p.hash]
		if !ok {
			continue
		}
		grp.Go(func() error {
			full := filepath.Join(workDir, p.path)
			cur, err := g.casfs.Hash(full)
			if err != nil {
				return nil // Deleted or replaced locally.
			}
			switch cur {
			case f.Pointer:
				st, err := os.Lstat(full)
				if err != nil {
					return err
				}
				// The size lets CASFS fetch the content lazily, on first read.
				if err := g.casfs.SetHashSize(full, f.Content, f.Size); err != nil {
					return fmt.Errorf("cannot hydrate %s: %w", p.path, err)
				}
				// Keep the mtime in a past second, so the index written next
				// has no racily clean entry to wait out or rehash.
				mtime := st.ModTime()
				if now := time.Now(); mtime.Unix() >= now.Unix() {
					mtime = time.Unix(now.Unix()-1, 0)
				}
				if err := os.Chtimes(full, mtime, mtime); err != nil {
					return fmt.Errorf("cannot set mtime of %s: %w", p.path, err)
				}
			case f.Content:
			default:
				return nil // Local edit.
			}
			mu.Lock()
			state[p.path] = f
			mu.Unlock()
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	g.addLFSDigests(ctx, state)
	return writeLFSState(gitDir, state)
}

// addLFSDigests tells the mount the SHA-256 of each hydrated file's content,
// which is its oid, so tools reading digests from CASFS need not hash it.
func (g *CASGit) addLFSDigests(ctx context.Context, state lfsState) {
	if g.client == nil || len(state) == 0 {
		return
	}
	seen := make(map[entities.Hash]bool, len(state))
	entries := make([]binding.DigestEntry, 0, len(state))
	for _, f := range state {
		if seen[f.Content] {
			continue
		}
		seen[f.Content] = true
		sha, err := hex.DecodeString(f.OID)
		if err != nil || len(sha) != entities.SHA256Size {
			continue
		}
		entries = append(entries, binding.DigestEntry{Hash: f.Content, SHA256: entities.SHA256(sha)})
	}
	if err := g.client.AddDigestMapEntries(ctx, entries); err != nil {
		log.Printf("cannot record Git LFS digests: %v", err)
	}
}
