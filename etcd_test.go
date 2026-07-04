// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// bg returns a cancellable context and its cancel func for a test.
func bg(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithCancel(context.Background())
}

// waitForWatchers blocks until the fake has at least n registered watchers.
func waitForWatchers(t *testing.T, f *fakeEtcd, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.mu.Lock()
		got := len(f.watchers)
		f.mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("watchers: got %d, want >= %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// --- client.go ---

func TestNewSuccessAndClose(t *testing.T) {
	// Multiple endpoint forms exercise stripScheme's three branches.
	c, err := New(Config{
		Endpoints:   []string{"http://127.0.0.1:2379", "https://127.0.0.1:2380", "127.0.0.1:2381"},
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewNoEndpoints(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty endpoints")
	}
}

func TestCloseTransport(t *testing.T) {
	f := newFake()
	f.errClose = errors.New("boom")
	if err := newWithTransport(f).Close(); err == nil {
		t.Fatal("expected close error")
	}
}

// --- errors.go ---

func TestErrorFormatting(t *testing.T) {
	if got := ErrCanceled.Error(); got != "etcd: Canceled (Canceled)" {
		t.Fatalf("no-message format: %q", got)
	}
	e := mapError(status.Error(codes.NotFound, "nope"))
	if got := e.Error(); got != "etcd: NotFound (NotFound): nope" {
		t.Fatalf("message format: %q", got)
	}
}

func TestMapError(t *testing.T) {
	if mapError(nil) != nil {
		t.Fatal("nil should map to nil")
	}
	// Already an *Error: returned unchanged.
	if got := mapError(ErrNotFound); got != ErrNotFound {
		t.Fatalf("already-Error: %v", got)
	}
	// Unknown code falls back to the Unknown name.
	e := mapError(status.Error(codes.Code(42), "x")).(*Error)
	if e.Name != "Unknown" {
		t.Fatalf("unknown-code name: %q", e.Name)
	}
}

func TestErrorIsAndUnwrap(t *testing.T) {
	orig := status.Error(codes.NotFound, "x")
	mapped := mapError(orig)
	if !errors.Is(mapped, ErrNotFound) {
		t.Fatal("should match ErrNotFound")
	}
	if errors.Is(mapped, ErrCanceled) {
		t.Fatal("should not match ErrCanceled")
	}
	if ErrNotFound.Is(errors.New("plain")) {
		t.Fatal("non-Error target should not match")
	}
	if errors.Unwrap(mapped) == nil {
		t.Fatal("mapped error should unwrap to cause")
	}
	if errors.Unwrap(ErrNotFound) != nil {
		t.Fatal("sentinel should unwrap to nil")
	}
}

// --- kv.go ---

func TestPutGetDelExists(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)

	// Empty-key guards.
	if _, err := c.Put(ctx, "", "v"); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("put empty: %v", err)
	}
	if _, err := c.Get(ctx, ""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("get empty: %v", err)
	}
	if _, err := c.Del(ctx, ""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("del empty: %v", err)
	}
	if _, err := c.Exists(ctx, ""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("exists empty: %v", err)
	}

	// Put fresh: no previous kv.
	pr, err := c.Put(ctx, "a", "1", WithLease(0), WithPrevKV())
	if err != nil {
		t.Fatal(err)
	}
	if pr.PrevKv != nil {
		t.Fatal("fresh put should have no prev")
	}
	if pr.Header.Revision == 0 {
		t.Fatal("header revision should be set")
	}

	// Overwrite: previous kv surfaced.
	pr, err = c.Put(ctx, "a", "2", WithPrevKV())
	if err != nil || pr.PrevKv == nil || pr.PrevKv.Value != "1" {
		t.Fatalf("overwrite prev: %+v err=%v", pr, err)
	}

	// Get single.
	gr, err := c.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if gr.Count != 1 || gr.First().Value != "2" {
		t.Fatalf("get single: %+v", gr)
	}

	// Empty result First() is nil.
	if empty := (&GetResult{}); empty.First() != nil {
		t.Fatal("empty First should be nil")
	}

	// Exists.
	if ok, _ := c.Exists(ctx, "a"); !ok {
		t.Fatal("a should exist")
	}
	if ok, _ := c.Exists(ctx, "missing"); ok {
		t.Fatal("missing should not exist")
	}

	// Del with prev.
	dr, err := c.Del(ctx, "a", WithPrevKV())
	if err != nil {
		t.Fatal(err)
	}
	if dr.Deleted != 1 || len(dr.PrevKvs) != 1 || dr.PrevKvs[0].Value != "2" {
		t.Fatalf("del: %+v", dr)
	}
}

func TestGetRangeOptions(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)
	for _, k := range []string{"k1", "k2", "k3"} {
		if _, err := c.Put(ctx, k, k); err != nil {
			t.Fatal(err)
		}
	}
	// Prefix.
	gr, _ := c.Get(ctx, "k", WithPrefix())
	if len(gr.Kvs) != 3 {
		t.Fatalf("prefix: %d", len(gr.Kvs))
	}
	// Range [k1,k3): k1,k2.
	gr, _ = c.Get(ctx, "k1", WithRange("k3"))
	if len(gr.Kvs) != 2 {
		t.Fatalf("range: %d", len(gr.Kvs))
	}
	// FromKey.
	gr, _ = c.Get(ctx, "k2", WithFromKey())
	if len(gr.Kvs) != 2 {
		t.Fatalf("fromkey: %d", len(gr.Kvs))
	}
	// Limit + More.
	gr, _ = c.Get(ctx, "k", WithPrefix(), WithLimit(2))
	if len(gr.Kvs) != 2 || !gr.More {
		t.Fatalf("limit: %+v", gr)
	}
	// CountOnly.
	gr, _ = c.Get(ctx, "k", WithPrefix(), WithCountOnly())
	if gr.Count != 3 || len(gr.Kvs) != 0 {
		t.Fatalf("countonly: %+v", gr)
	}
	// KeysOnly.
	gr, _ = c.Get(ctx, "k", WithPrefix(), WithKeysOnly())
	if gr.Kvs[0].Value != "" {
		t.Fatal("keysonly should drop values")
	}
	// Revision + Serializable options simply pass through.
	if _, err := c.Get(ctx, "k1", WithRevision(1), WithSerializable()); err != nil {
		t.Fatal(err)
	}
}

func TestKVErrors(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)
	f.errGet = errors.New("g")
	f.errPut = errors.New("p")
	f.errDelete = errors.New("d")
	if _, err := c.Get(ctx, "k"); err == nil {
		t.Fatal("get err")
	}
	if _, err := c.Exists(ctx, "k"); err == nil {
		t.Fatal("exists err")
	}
	if _, err := c.Put(ctx, "k", "v"); err == nil {
		t.Fatal("put err")
	}
	if _, err := c.Del(ctx, "k"); err == nil {
		t.Fatal("del err")
	}
}

func TestMappingHelperEdges(t *testing.T) {
	if h := toHeader(nil); h != (Header{}) {
		t.Fatal("nil header should be zero")
	}
	if toKeyValuePtr(nil) != nil {
		t.Fatal("nil kv ptr")
	}
	if toKeyValues(nil) != nil {
		t.Fatal("nil kvs")
	}
}

// --- lease.go ---

func TestLease(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	f.ttlGrant = 30
	c := newWithTransport(f)

	gr, err := c.LeaseGrant(ctx, 30)
	if err != nil || gr.ID == 0 || gr.TTL != 30 {
		t.Fatalf("grant: %+v err=%v", gr, err)
	}
	ka, err := c.LeaseKeepAliveOnce(ctx, gr.ID)
	if err != nil || ka.ID != gr.ID {
		t.Fatalf("keepalive: %+v err=%v", ka, err)
	}
	// TTL with attached keys.
	f.ttlKeys = [][]byte{[]byte("x"), []byte("y")}
	ttl, err := c.LeaseTTL(ctx, gr.ID, true)
	if err != nil || len(ttl.Keys) != 2 || ttl.GrantedTTL != 30 {
		t.Fatalf("ttl keys: %+v err=%v", ttl, err)
	}
	// TTL without keys.
	f.ttlKeys = nil
	ttl, err = c.LeaseTTL(ctx, gr.ID, false)
	if err != nil || len(ttl.Keys) != 0 {
		t.Fatalf("ttl nokeys: %+v err=%v", ttl, err)
	}
	if _, err := c.LeaseRevoke(ctx, gr.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
}

func TestLeaseErrors(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)
	f.errGrant = errors.New("x")
	f.errKA = errors.New("x")
	f.errRevoke = errors.New("x")
	f.errTTL = errors.New("x")
	if _, err := c.LeaseGrant(ctx, 1); err == nil {
		t.Fatal("grant")
	}
	if _, err := c.LeaseKeepAliveOnce(ctx, 1); err == nil {
		t.Fatal("ka")
	}
	if _, err := c.LeaseRevoke(ctx, 1); err == nil {
		t.Fatal("revoke")
	}
	if _, err := c.LeaseTTL(ctx, 1, true); err == nil {
		t.Fatal("ttl")
	}
}

// --- txn.go ---

func TestTransactionSuccess(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)
	if _, err := c.Put(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	res, err := c.Transaction(ctx, func(tx *Txn) {
		tx.If(Compare(Value("k"), "=", "v")).
			Then(OpPut("k", "v2"), OpGet("k"), OpDelete("gone")).
			Else(OpPut("k", "fail"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Succeeded {
		t.Fatal("should succeed")
	}
	if len(res.Responses) != 3 {
		t.Fatalf("responses: %d", len(res.Responses))
	}
	if res.Responses[0].Put == nil || res.Responses[1].Get == nil || res.Responses[2].Del == nil {
		t.Fatalf("response kinds: %+v", res.Responses)
	}
	if res.Responses[1].Get.First().Value != "v2" {
		t.Fatal("get in txn should see v2")
	}
}

func TestTransactionFailure(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)
	res, err := c.Transaction(ctx, func(tx *Txn) {
		tx.If(Compare(Value("absent"), "=", "x")).
			Then(OpPut("a", "1")).
			Else(OpPut("b", "2"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Succeeded {
		t.Fatal("should fail comparison")
	}
	gr, _ := c.Get(ctx, "b")
	if gr.First() == nil || gr.First().Value != "2" {
		t.Fatal("else branch should have run")
	}
}

func TestTransactionCompareTargets(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)
	if _, err := c.Put(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	gr, _ := c.Get(ctx, "k")
	kv := gr.First()

	cases := []struct {
		name string
		cmp  Cmp
		want bool
	}{
		{"version", Compare(Version("k"), "=", kv.Version), true},
		{"create", Compare(CreateRevision("k"), "=", kv.CreateRevision), true},
		{"mod", Compare(ModRevision("k"), ">", int64(0)), true},
		{"lease", Compare(LeaseValue("k"), "=", int64(0)), true},
		{"value-neq", Compare(Value("k"), "!=", "other"), true},
		{"version-less", Compare(Version("k"), "<", int64(0)), false},
	}
	for _, tc := range cases {
		res, err := c.Transaction(ctx, func(tx *Txn) { tx.If(tc.cmp).Then(OpGet("k")) })
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if res.Succeeded != tc.want {
			t.Fatalf("%s: got %v want %v", tc.name, res.Succeeded, tc.want)
		}
	}
}

func TestTransactionError(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	f.errTxn = errors.New("boom")
	c := newWithTransport(f)
	if _, err := c.Transaction(ctx, func(tx *Txn) { tx.If() }); err == nil {
		t.Fatal("expected txn error")
	}
}

// --- watch.go ---

func TestWatchDelivery(t *testing.T) {
	ctx, cancel := bg(t)
	f := newFake()
	c := newWithTransport(f)
	out := c.Watch(ctx, "w", WithPrefix())
	waitForWatchers(t, f, 1)

	if _, err := c.Put(ctx, "wa", "1"); err != nil {
		t.Fatal(err)
	}
	r := <-out
	if len(r.Events) != 1 || r.Events[0].Type != EventPut || r.Events[0].Kv.Key != "wa" {
		t.Fatalf("put event: %+v", r)
	}
	if r.Events[0].PrevKv != nil {
		t.Fatal("fresh put has no prev")
	}

	if _, err := c.Del(ctx, "wa"); err != nil {
		t.Fatal(err)
	}
	r = <-out
	if r.Events[0].Type != EventDelete || r.Events[0].PrevKv == nil {
		t.Fatalf("delete event: %+v", r)
	}
	cancel()
}

func TestEventTypeString(t *testing.T) {
	if EventPut.String() != "PUT" || EventDelete.String() != "DELETE" {
		t.Fatal("event type strings")
	}
}

// watchStub overrides Watch with a caller-controlled channel; other transport
// methods are promoted from the embedded fake.
type watchStub struct {
	*fakeEtcd
	ch chan clientv3.WatchResponse
}

func (w watchStub) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return w.ch
}

func TestWatchContextCancelDuringSend(t *testing.T) {
	wch := make(chan clientv3.WatchResponse)
	c := newWithTransport(watchStub{fakeEtcd: newFake(), ch: wch})
	ctx, cancel := context.WithCancel(context.Background())
	out := c.Watch(ctx, "k")
	// Deliver a response; the forwarding goroutine receives it and then blocks
	// trying to send on the unread out channel.
	wch <- clientv3.WatchResponse{}
	cancel()        // unblocks the goroutine via the ctx.Done branch of its select
	for range out { // drains until the goroutine returns and closes out
	}
}

func TestWatchBlock(t *testing.T) {
	ctx, cancel := bg(t)
	f := newFake()
	c := newWithTransport(f)

	got := make(chan Event, 1)
	done := make(chan error, 1)
	go func() {
		done <- c.WatchBlock(ctx, func(evs []Event) { got <- evs[0] }, "b", WithPrefix())
	}()
	waitForWatchers(t, f, 1)
	if _, err := c.Put(ctx, "bx", "1"); err != nil {
		t.Fatal(err)
	}
	ev := <-got
	if ev.Kv.Key != "bx" {
		t.Fatalf("event: %+v", ev)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("clean stop should be nil: %v", err)
	}
}

func TestWatchBlockSkipEmptyAndError(t *testing.T) {
	wch := make(chan clientv3.WatchResponse, 2)
	wch <- clientv3.WatchResponse{Created: true}      // no events, no err: skipped
	wch <- clientv3.WatchResponse{CompactRevision: 5} // terminal error
	close(wch)
	c := newWithTransport(watchStub{fakeEtcd: newFake(), ch: wch})
	err := c.WatchBlock(context.Background(), func([]Event) { t.Fatal("fn should not run") }, "k")
	if err == nil {
		t.Fatal("expected terminal watch error")
	}
}

// --- lock.go ---

func TestLockImmediate(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)
	lk, err := c.Lock(ctx, "res", 10)
	if err != nil {
		t.Fatal(err)
	}
	if lk.Key == "" || lk.Lease == 0 {
		t.Fatalf("lock: %+v", lk)
	}
	if err := c.Unlock(ctx, lk); err != nil {
		t.Fatal(err)
	}
}

func TestLockWaitsForPredecessor(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	c := newWithTransport(f)
	// Seed a predecessor with a lower creation revision under the lock prefix.
	if _, err := c.Put(ctx, "res/0000", "held"); err != nil {
		t.Fatal(err)
	}

	type result struct {
		lk  *Lock
		err error
	}
	res := make(chan result, 1)
	go func() {
		lk, err := c.Lock(ctx, "res", 10)
		res <- result{lk, err}
	}()
	waitForWatchers(t, f, 1) // Lock is now blocked in waitDelete
	if _, err := c.Del(ctx, "res/0000"); err != nil {
		t.Fatal(err)
	}
	r := <-res
	if r.err != nil {
		t.Fatalf("lock should acquire after predecessor removed: %v", r.err)
	}
	if err := c.Unlock(ctx, r.lk); err != nil {
		t.Fatal(err)
	}
}

func TestLockErrors(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()

	// Grant failure.
	f := newFake()
	f.errGrant = errors.New("g")
	if _, err := newWithTransport(f).Lock(ctx, "r", 1); err == nil {
		t.Fatal("grant error")
	}

	// Txn failure (grant ok).
	f = newFake()
	f.errTxn = errors.New("t")
	if _, err := newWithTransport(f).Lock(ctx, "r", 1); err == nil {
		t.Fatal("txn error")
	}

	// Get failure in the wait loop (grant+txn ok, no get in txn since cmp holds).
	f = newFake()
	f.errGet = errors.New("g")
	if _, err := newWithTransport(f).Lock(ctx, "r", 1); err == nil {
		t.Fatal("get error")
	}

	// waitDelete failure: a predecessor exists and the watch reports an error.
	f = newFake()
	f.watchErr = true
	c := newWithTransport(f)
	if _, err := c.Put(ctx, "r/0000", "held"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Lock(ctx, "r", 1); err == nil {
		t.Fatal("waitDelete error")
	}
}

func TestUnlockErrors(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()

	f := newFake()
	f.errDelete = errors.New("d")
	if err := newWithTransport(f).Unlock(ctx, &Lock{Key: "k", Lease: 1}); err == nil {
		t.Fatal("delete error")
	}

	f = newFake()
	f.errRevoke = errors.New("r")
	if err := newWithTransport(f).Unlock(ctx, &Lock{Key: "k", Lease: 1}); err == nil {
		t.Fatal("revoke error")
	}
}

func TestWaitDeleteBranches(t *testing.T) {
	// Channel closes without a delete: returns ctx.Err().
	ch := make(chan clientv3.WatchResponse)
	close(ch)
	c := newWithTransport(watchStub{fakeEtcd: newFake(), ch: ch})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.waitDelete(ctx, "k"); err == nil {
		t.Fatal("closed channel should return ctx error")
	}
}

// --- maintenance.go ---

func TestMembersAndStatus(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	f.members = []*pb.Member{{ID: 10, Name: "m1", PeerURLs: []string{"p"}, ClientURLs: []string{"c"}, IsLearner: true}}
	f.status = &pb.StatusResponse{Version: "3.6.0", DbSize: 100, Leader: 10, RaftIndex: 5, RaftTerm: 2, IsLearner: false}
	c := newWithTransport(f)

	mr, err := c.Members(ctx)
	if err != nil || len(mr.Members) != 1 || mr.Members[0].Name != "m1" || !mr.Members[0].IsLearner {
		t.Fatalf("members: %+v err=%v", mr, err)
	}
	sr, err := c.Status(ctx, "127.0.0.1:2379")
	if err != nil || sr.Version != "3.6.0" || sr.Leader != 10 {
		t.Fatalf("status: %+v err=%v", sr, err)
	}

	// Empty members and default (nil-preset) status.
	f.members = nil
	f.status = nil
	mr, _ = c.Members(ctx)
	if len(mr.Members) != 0 {
		t.Fatal("expected no members")
	}
	if _, err := c.Status(ctx, "x"); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceErrors(t *testing.T) {
	ctx, cancel := bg(t)
	defer cancel()
	f := newFake()
	f.errMember = errors.New("m")
	f.errStatus = errors.New("s")
	c := newWithTransport(f)
	if _, err := c.Members(ctx); err == nil {
		t.Fatal("members error")
	}
	if _, err := c.Status(ctx, "x"); err == nil {
		t.Fatal("status error")
	}
}
