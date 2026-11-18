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

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ephemeralKey = "csi.storage.k8s.io/ephemeral"
	// Optional volume attributes for the workspace root's owner and mode.
	uidKey  = "uid"
	gidKey  = "gid"
	modeKey = "mode"
)

var version = "dev"

type driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	name     string
	nodeID   string
	abfs     string
	stateDir string

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// volumeState is what the driver remembers about a published volume, so it
// can clean up after a restart.
type volumeState struct {
	VolumeID string `json:"volume_id"`
	Target   string `json:"target"`
	Cache    string `json:"cache"`
	Scratch  string `json:"scratch"`
	Pod      string `json:"pod,omitempty"`
}

func (d *driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: d.name, VendorVersion: version}, nil
}

func (d *driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	// Node service only: no controller, provisioning or attach.
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (d *driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	if _, err := os.Stat("/sys/module/casfs"); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "casfs kernel module is not loaded")
	}
	return &csi.ProbeResponse{}, nil
}

func (d *driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: d.nodeID}, nil
}

func (d *driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// No staging, stats or expansion.
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

func (d *driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	id, target := req.GetVolumeId(), req.GetTargetPath()
	switch {
	case id == "" || target == "":
		return nil, status.Error(codes.InvalidArgument, "volume ID and target path are required")
	case req.GetVolumeCapability().GetMount() == nil:
		return nil, status.Error(codes.InvalidArgument, "only filesystem volumes are supported")
	case req.GetVolumeContext()[ephemeralKey] != "true":
		return nil, status.Error(codes.InvalidArgument, "only inline ephemeral volumes are supported")
	case req.GetReadonly():
		return nil, status.Error(codes.InvalidArgument, "workspaces are writable")
	}
	vc := req.GetVolumeContext()
	uid, gid, mode, err := ownership(vc)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	unlock := d.lock(id)
	defer unlock()

	if m, ok := casfsMountAt(target); ok {
		log.Printf("volume %s already published at %s (scratch %s)", id, target, m.scratch)
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if err := os.MkdirAll(target, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "cannot create target: %v", err)
	}

	start := time.Now()
	// --daemon returns once the mount and its control socket are up.
	if out, err := d.run(ctx, "casfs-mount", "--daemon", target); err != nil {
		d.teardown(context.Background(), target)
		return nil, status.Errorf(codes.Internal, "casfs-mount: %v: %s", err, out)
	}
	m, ok := casfsMountAt(target)
	if !ok {
		d.teardown(context.Background(), target)
		return nil, status.Error(codes.Internal, "casfs-mount succeeded but no casfs mount is at the target")
	}
	if err := os.Chown(target, uid, gid); err != nil {
		d.teardown(context.Background(), target)
		return nil, status.Errorf(codes.Internal, "cannot chown workspace: %v", err)
	}
	if err := os.Chmod(target, mode); err != nil {
		d.teardown(context.Background(), target)
		return nil, status.Errorf(codes.Internal, "cannot chmod workspace: %v", err)
	}

	st := volumeState{
		VolumeID: id,
		Target:   target,
		Cache:    m.cache,
		Scratch:  m.scratch,
		Pod:      vc["csi.storage.k8s.io/pod.namespace"] + "/" + vc["csi.storage.k8s.io/pod.name"],
	}
	if err := d.saveState(st); err != nil {
		log.Printf("cannot record volume %s: %v", id, err)
	}
	log.Printf("published volume %s for %s at %s (scratch %s) in %s", id, st.Pod, target, m.scratch, time.Since(start).Round(time.Millisecond))
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	id, target := req.GetVolumeId(), req.GetTargetPath()
	if id == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID and target path are required")
	}
	unlock := d.lock(id)
	defer unlock()

	st, _ := d.loadState(id)
	if m, ok := casfsMountAt(target); ok {
		if st.Scratch == "" {
			st.Cache, st.Scratch = m.cache, m.scratch
		}
	}
	if err := d.teardown(ctx, target); err != nil {
		return nil, status.Errorf(codes.Internal, "cannot unmount %s: %v", target, err)
	}
	removeScratch(st)
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.Internal, "cannot remove target: %v", err)
	}
	d.deleteState(id)
	log.Printf("unpublished volume %s from %s", id, target)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// teardown stops the mount daemon and unmounts target, falling back to
// casfs-mounter when the daemon is gone. It fails while target is mounted.
func (d *driver) teardown(ctx context.Context, target string) error {
	if _, ok := casfsMountAt(target); !ok {
		return nil
	}
	if out, err := d.run(ctx, "unmount", target); err != nil {
		log.Printf("abfs unmount %s: %v: %s", target, err, out)
	}
	for i := 0; i < 20; i++ {
		if _, ok := casfsMountAt(target); !ok {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The daemon may have died; unmount the kernel filesystem directly.
	out, err := exec.CommandContext(ctx, "casfs-mounter", "-u", target).CombinedOutput()
	if _, ok := casfsMountAt(target); ok {
		return fmt.Errorf("still mounted after casfs-mounter -u: %v: %s", err, out)
	}
	return nil
}

// removeScratch deletes a mount's scratch directory, its local writes, from
// the shared cache. The content cache itself stays.
func removeScratch(st volumeState) {
	if st.Cache == "" || st.Scratch == "" || strings.ContainsAny(st.Scratch, "/.") {
		return
	}
	dir := filepath.Join(st.Cache, "scratch", st.Scratch)
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("cannot remove scratch %s: %v", dir, err)
	}
}

// reconcile drops bookkeeping for volumes that are no longer mounted, which
// happens when the plugin restarts after kubelet already cleaned up.
func (d *driver) reconcile() {
	entries, err := os.ReadDir(d.stateDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		id, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok {
			continue
		}
		st, err := d.loadState(id)
		if err != nil {
			continue
		}
		if _, mounted := casfsMountAt(st.Target); mounted {
			log.Printf("volume %s still mounted at %s", id, st.Target)
			continue
		}
		removeScratch(st)
		d.deleteState(id)
		log.Printf("dropped stale volume %s", id)
	}
}

func (d *driver) run(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return exec.CommandContext(ctx, d.abfs, args...).CombinedOutput()
}

func (d *driver) lock(id string) func() {
	d.mu.Lock()
	if d.locks == nil {
		d.locks = make(map[string]*sync.Mutex)
	}
	l, ok := d.locks[id]
	if !ok {
		l = new(sync.Mutex)
		d.locks[id] = l
	}
	d.mu.Unlock()
	l.Lock()
	return l.Unlock
}

func (d *driver) statePath(id string) string {
	return filepath.Join(d.stateDir, strings.ReplaceAll(id, "/", "_")+".json")
}

func (d *driver) saveState(st volumeState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := d.statePath(st.VolumeID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, d.statePath(st.VolumeID))
}

func (d *driver) loadState(id string) (volumeState, error) {
	var st volumeState
	data, err := os.ReadFile(d.statePath(id))
	if err != nil {
		return st, err
	}
	return st, json.Unmarshal(data, &st)
}

func (d *driver) deleteState(id string) {
	if err := os.Remove(d.statePath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("cannot remove state for %s: %v", id, err)
	}
}

func ownership(vc map[string]string) (uid, gid int, mode os.FileMode, err error) {
	parse := func(key string, base int, def int64) (int64, error) {
		v, ok := vc[key]
		if !ok {
			return def, nil
		}
		n, err := strconv.ParseInt(v, base, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %s %q", key, v)
		}
		return n, nil
	}
	u, err := parse(uidKey, 10, 0)
	if err != nil {
		return
	}
	g, err := parse(gidKey, 10, 0)
	if err != nil {
		return
	}
	m, err := parse(modeKey, 8, 0o755)
	if err != nil {
		return
	}
	return int(u), int(g), os.FileMode(m) & os.ModePerm, nil
}

type casfsMount struct {
	cache   string
	scratch string
}

// casfsMountAt reports the CASFS mount at path, from /proc/self/mountinfo.
func casfsMountAt(path string) (casfsMount, bool) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return casfsMount{}, false
	}
	defer f.Close()
	path = filepath.Clean(path)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		// id parent major:minor root mountpoint options ... - fstype source superoptions
		pre, post, ok := strings.Cut(sc.Text(), " - ")
		if !ok {
			continue
		}
		fields, sfields := strings.Fields(pre), strings.Fields(post)
		if len(fields) < 5 || len(sfields) < 3 || sfields[0] != "casfs" || unescape(fields[4]) != path {
			continue
		}
		var m casfsMount
		for _, opt := range strings.Split(sfields[2], ",") {
			if v, ok := strings.CutPrefix(opt, "cache="); ok {
				m.cache = unescape(v)
			} else if v, ok := strings.CutPrefix(opt, "scratch="); ok {
				m.scratch = v
			}
		}
		return m, true
	}
	return casfsMount{}, false
}

// unescape undoes the octal escapes mountinfo uses for spaces and the like.
func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
