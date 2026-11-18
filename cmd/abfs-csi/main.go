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

// Command abfs-csi is a Kubernetes CSI node plugin that gives each pod an
// empty, writable ABFS workspace as an inline ephemeral volume. Each volume
// is a CASFS mount with its own ABFS mount daemon, backed by the node's
// shared Cacheman instance, so jobs on a node reuse one content cache.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

func main() {
	endpoint := flag.String("endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
	nodeID := flag.String("node-id", os.Getenv("NODE_NAME"), "Kubernetes node name")
	driverName := flag.String("driver-name", "abfs.csi.doordash.com", "CSI driver name")
	abfsBin := flag.String("abfs", "abfs", "Path to the abfs binary")
	stateDir := flag.String("state-dir", "/var/lib/abfs-csi", "Directory for per-volume bookkeeping")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("--node-id or NODE_NAME is required")
	}
	if os.Getenv("ABFS_BASE_DIR") == "" {
		log.Fatal("ABFS_BASE_DIR must point at the node's ABFS instance directory")
	}
	if err := os.MkdirAll(*stateDir, 0o700); err != nil {
		log.Fatalf("cannot create state dir: %v", err)
	}

	d := &driver{
		name:     *driverName,
		nodeID:   *nodeID,
		abfs:     *abfsBin,
		stateDir: *stateDir,
	}
	d.reconcile()

	lis, err := listen(*endpoint)
	if err != nil {
		log.Fatalf("cannot listen on %s: %v", *endpoint, err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(logErrors))
	csi.RegisterIdentityServer(srv, d)
	csi.RegisterNodeServer(srv, d)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	log.Printf("abfs-csi %s serving on %s for node %s", d.name, *endpoint, d.nodeID)
	if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatalf("serve: %v", err)
	}
}

func listen(endpoint string) (net.Listener, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "unix" {
		return nil, errors.New("only unix:// endpoints are supported")
	}
	sock := u.Path
	if sock == "" {
		sock = u.Opaque
	}
	if err := os.MkdirAll(filepath.Dir(sock), 0o750); err != nil {
		return nil, err
	}
	if err := os.Remove(sock); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return net.Listen("unix", sock)
}

func logErrors(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		log.Printf("%s failed: %v", info.FullMethod, err)
	}
	return resp, err
}
