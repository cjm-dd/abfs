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
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func TestUnescape(t *testing.T) {
	for in, want := range map[string]string{
		`/plain/path`:       "/plain/path",
		`/with\040space`:    "/with space",
		`/tab\011and\134bs`: "/tab\tand\\bs",
	} {
		if got := unescape(in); got != want {
			t.Errorf("unescape(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestOwnership(t *testing.T) {
	uid, gid, mode, err := ownership(map[string]string{})
	if err != nil || uid != 0 || gid != 0 || mode != 0o755 {
		t.Errorf("defaults = %d, %d, %o, %v", uid, gid, mode, err)
	}
	uid, gid, mode, err = ownership(map[string]string{"uid": "1000", "gid": "100", "mode": "0770"})
	if err != nil || uid != 1000 || gid != 100 || mode != 0o770 {
		t.Errorf("explicit = %d, %d, %o, %v", uid, gid, mode, err)
	}
	if _, _, _, err := ownership(map[string]string{"mode": "rwx"}); err == nil {
		t.Error("invalid mode accepted")
	}
}

func publishRequest(id, target string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:   id,
		TargetPath: target,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		},
		VolumeContext: map[string]string{
			ephemeralKey: "true",
			uidKey:       strconv.Itoa(os.Getuid()),
			gidKey:       strconv.Itoa(os.Getgid()),
		},
	}
}

func TestRejectsNonEphemeral(t *testing.T) {
	d := &driver{stateDir: t.TempDir()}
	req := publishRequest("v", filepath.Join(t.TempDir(), "mnt"))
	delete(req.VolumeContext, ephemeralKey)
	if _, err := d.NodePublishVolume(context.Background(), req); err == nil {
		t.Fatal("persistent volume accepted")
	}
}

// TestPublishUnpublish needs a loaded casfs module, casfs-mounter on PATH,
// a running Cacheman for ABFS_BASE_DIR/ABFS_INSTANCE, and
// ABFS_CSI_INTEGRATION=1.
func TestPublishUnpublish(t *testing.T) {
	if os.Getenv("ABFS_CSI_INTEGRATION") != "1" {
		t.Skip("set ABFS_CSI_INTEGRATION=1 to run against a live CASFS setup")
	}
	ctx := context.Background()
	d := &driver{abfs: "abfs", stateDir: t.TempDir()}
	target := filepath.Join(os.Getenv("ABFS_CSI_TEST_DIR"), "vol")
	req := publishRequest("csi-test-volume", target)

	if _, err := d.NodePublishVolume(ctx, req); err != nil {
		t.Fatalf("publish: %v", err)
	}
	m, ok := casfsMountAt(target)
	if !ok || m.scratch == "" || m.cache == "" {
		t.Fatalf("no casfs mount at %s: %+v", target, m)
	}
	if _, err := os.Stat(filepath.Join(target, ".abfs.sock")); err != nil {
		t.Errorf("mount control socket missing: %v", err)
	}
	// Publishing again is a no-op.
	if _, err := d.NodePublishVolume(ctx, req); err != nil {
		t.Errorf("second publish: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "written"), []byte("data"), 0o644); err != nil {
		t.Errorf("workspace not writable: %v", err)
	}
	scratch := filepath.Join(m.cache, "scratch", m.scratch)

	unpub := &csi.NodeUnpublishVolumeRequest{VolumeId: req.VolumeId, TargetPath: target}
	if _, err := d.NodeUnpublishVolume(ctx, unpub); err != nil {
		t.Fatalf("unpublish: %v", err)
	}
	if _, ok := casfsMountAt(target); ok {
		t.Error("still mounted after unpublish")
	}
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Errorf("scratch %s left behind: %v", scratch, err)
	}
	// Unpublishing again is a no-op.
	if _, err := d.NodeUnpublishVolume(ctx, unpub); err != nil {
		t.Errorf("second unpublish: %v", err)
	}
}
