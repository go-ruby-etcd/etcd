// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

// Package live drives the go-ruby-etcd/etcd Client against a real, in-process
// etcd started with go.etcd.io/etcd/server/v3/embed. It lives in its own nested
// module so the embedded server's large dependency tree never enters the main
// module's go.mod: the main module's default suite reaches 100% coverage on
// every arch under qemu against a deterministic in-memory transport, while this
// suite validates real round-trip behaviour on native lanes. Run it with:
//
//	cd live && go test ./...
//
// It complements the deterministic suite by exercising genuine etcd semantics:
// prefix reads, watch delivery, lease grant/keepalive/attach, transactions and
// the lease-backed lock.
package live

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	etcd "github.com/go-ruby-etcd/etcd"
	"go.etcd.io/etcd/server/v3/embed"
)

// freePort returns an available localhost TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startEmbedded boots an in-process etcd and returns a Client bound to it plus
// the client endpoint it serves.
func startEmbedded(t *testing.T) (*etcd.Client, string) {
	t.Helper()
	cport, pport := freePort(t), freePort(t)
	curl, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", cport))
	purl, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", pport))

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{*curl}
	cfg.AdvertiseClientUrls = []url.URL{*curl}
	cfg.ListenPeerUrls = []url.URL{*purl}
	cfg.AdvertisePeerUrls = []url.URL{*purl}
	cfg.InitialCluster = cfg.Name + "=" + purl.String()

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start embedded etcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		e.Close()
		t.Fatal("embedded etcd did not become ready")
	}
	t.Cleanup(e.Close)

	c, err := etcd.New(etcd.Config{Endpoints: []string{curl.Host}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, curl.Host
}

func TestLiveKVWatchLeaseTxnLock(t *testing.T) {
	c, endpoint := startEmbedded(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// KV: put/get/prefix/delete/exists.
	if _, err := c.Put(ctx, "/a/1", "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put(ctx, "/a/2", "two"); err != nil {
		t.Fatal(err)
	}
	gr, err := c.Get(ctx, "/a/", etcd.WithPrefix())
	if err != nil || len(gr.Kvs) != 2 {
		t.Fatalf("prefix get: %+v err=%v", gr, err)
	}
	if ok, _ := c.Exists(ctx, "/a/1"); !ok {
		t.Fatal("exists")
	}
	if dr, err := c.Del(ctx, "/a/", etcd.WithPrefix()); err != nil || dr.Deleted != 2 {
		t.Fatalf("prefix del: %+v err=%v", dr, err)
	}

	// Watch delivery.
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	out := c.Watch(wctx, "/w")
	if _, err := c.Put(ctx, "/w", "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-out:
		if len(r.Events) == 0 || r.Events[0].Kv.Value != "hello" {
			t.Fatalf("watch event: %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no watch event")
	}

	// Lease grant + keepalive + attach + ttl.
	lease, err := c.LeaseGrant(ctx, 60)
	if err != nil || lease.ID == 0 {
		t.Fatalf("grant: %+v err=%v", lease, err)
	}
	if _, err := c.Put(ctx, "/leased", "v", etcd.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.LeaseKeepAliveOnce(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	ttl, err := c.LeaseTTL(ctx, lease.ID, true)
	if err != nil || ttl.TTL <= 0 || len(ttl.Keys) != 1 {
		t.Fatalf("ttl: %+v err=%v", ttl, err)
	}

	// Transaction: compare/success/failure.
	if _, err := c.Put(ctx, "/t", "v0"); err != nil {
		t.Fatal(err)
	}
	res, err := c.Transaction(ctx, func(tx *etcd.Txn) {
		tx.If(etcd.Compare(etcd.Value("/t"), "=", "v0")).
			Then(etcd.OpPut("/t", "v1")).
			Else(etcd.OpPut("/t", "vX"))
	})
	if err != nil || !res.Succeeded {
		t.Fatalf("txn: %+v err=%v", res, err)
	}
	if gr, _ := c.Get(ctx, "/t"); gr.First().Value != "v1" {
		t.Fatal("txn did not apply success branch")
	}

	// Lock / Unlock.
	lk, err := c.Lock(ctx, "/lock/name", 30)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if ok, _ := c.Exists(ctx, lk.Key); !ok {
		t.Fatal("lock key should exist")
	}
	if err := c.Unlock(ctx, lk); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if ok, _ := c.Exists(ctx, lk.Key); ok {
		t.Fatal("lock key should be gone after unlock")
	}

	// Maintenance.
	if mr, err := c.Members(ctx); err != nil || len(mr.Members) == 0 {
		t.Fatalf("members: %+v err=%v", mr, err)
	}
	if sr, err := c.Status(ctx, endpoint); err != nil || sr.Version == "" {
		t.Fatalf("status: %+v err=%v", sr, err)
	}
}
