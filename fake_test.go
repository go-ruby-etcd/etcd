// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"bytes"
	"context"
	"sort"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// fakeEtcd is a deterministic, in-process implementation of the transport seam.
// It is not a full etcd: it is a faithful-enough KV/watch/lease/txn stand-in
// that lets every line of the Client's request-building and response-mapping
// logic run with no external server and no cgo, so the suite holds 100% coverage
// on every arch under qemu. Real round-trip behaviour (lease expiry, cluster
// consensus) is validated separately by the "live" embedded-etcd suite.
type fakeEtcd struct {
	mu        sync.Mutex
	rev       int64
	store     map[string]*mvccpb.KeyValue
	nextLease int64
	watchers  []*fakeWatcher

	clusterID uint64
	memberID  uint64
	raftTerm  uint64

	// Injected outcomes for exercising error branches.
	errPut, errGet, errDelete              error
	errGrant, errRevoke, errKA, errTTL     error
	errMember, errStatus, errTxn, errClose error

	// Preset replies for maintenance and lease-ttl.
	members  []*pb.Member
	status   *pb.StatusResponse
	ttlKeys  [][]byte
	ttlGrant int64

	// watchErr, when set, makes each new watcher immediately receive a canceled
	// response so watch-error paths can be exercised.
	watchErr bool
}

func newFake() *fakeEtcd {
	return &fakeEtcd{
		store:     map[string]*mvccpb.KeyValue{},
		clusterID: 1,
		memberID:  2,
		raftTerm:  3,
	}
}

func (f *fakeEtcd) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: f.clusterID,
		MemberId:  f.memberID,
		Revision:  f.rev,
		RaftTerm:  f.raftTerm,
	}
}

// --- KV ---

func (f *fakeEtcd) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errPut != nil {
		return nil, f.errPut
	}
	prev := f.putLocked(key, val)
	return &clientv3.PutResponse{Header: f.header(), PrevKv: prev}, nil
}

// putLocked applies a put and returns the previous key-value (nil if absent).
func (f *fakeEtcd) putLocked(key, val string) *mvccpb.KeyValue {
	f.rev++
	var prev *mvccpb.KeyValue
	kv := &mvccpb.KeyValue{Key: []byte(key), Value: []byte(val), ModRevision: f.rev}
	if old, ok := f.store[key]; ok {
		cp := *old
		prev = &cp
		kv.CreateRevision = old.CreateRevision
		kv.Version = old.Version + 1
	} else {
		kv.CreateRevision = f.rev
		kv.Version = 1
	}
	f.store[key] = kv
	f.notifyLocked(mvccpb.PUT, kv, prev)
	return prev
}

func (f *fakeEtcd) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errGet != nil {
		return nil, f.errGet
	}
	op := clientv3.OpGet(key, opts...)
	matched := f.matchLocked(op)
	resp := &clientv3.GetResponse{Header: f.header(), Count: int64(len(matched))}
	if op.IsCountOnly() {
		return resp, nil
	}
	if lim := op.Limit(); lim > 0 && int64(len(matched)) > lim {
		matched = matched[:lim]
		resp.More = true
	}
	if op.IsKeysOnly() {
		for i := range matched {
			cp := *matched[i]
			cp.Value = nil
			matched[i] = &cp
		}
	}
	resp.Kvs = matched
	return resp, nil
}

func (f *fakeEtcd) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errDelete != nil {
		return nil, f.errDelete
	}
	op := clientv3.OpDelete(key, opts...)
	deleted, prevs := f.deleteLocked(op)
	return &clientv3.DeleteResponse{Header: f.header(), Deleted: deleted, PrevKvs: prevs}, nil
}

// deleteLocked removes every key the op selects and returns the count and the
// removed key-values.
func (f *fakeEtcd) deleteLocked(op clientv3.Op) (int64, []*mvccpb.KeyValue) {
	matched := f.matchLocked(op)
	if len(matched) == 0 {
		return 0, nil
	}
	f.rev++
	var prevs []*mvccpb.KeyValue
	for _, kv := range matched {
		prevs = append(prevs, kv)
		delete(f.store, string(kv.Key))
		tomb := &mvccpb.KeyValue{Key: kv.Key, ModRevision: f.rev}
		f.notifyLocked(mvccpb.DELETE, tomb, kv)
	}
	return int64(len(matched)), prevs
}

// matchLocked returns the stored key-values the op selects, sorted by key.
func (f *fakeEtcd) matchLocked(op clientv3.Op) []*mvccpb.KeyValue {
	key := string(op.KeyBytes())
	var lo, hi string
	single := false
	switch {
	case op.IsOptsWithPrefix():
		lo, hi = key, prefixEnd(key)
	case op.IsOptsWithFromKey():
		lo, hi = key, ""
	case len(op.RangeBytes()) > 0:
		lo, hi = key, string(op.RangeBytes())
	default:
		single = true
	}
	var out []*mvccpb.KeyValue
	for k, kv := range f.store {
		if single {
			if k == key {
				out = append(out, kv)
			}
			continue
		}
		if k < lo {
			continue
		}
		if hi != "" && k >= hi {
			continue
		}
		out = append(out, kv)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].Key) < string(out[j].Key) })
	return out
}

// prefixEnd returns the exclusive upper bound of the prefix range for prefix,
// or "" (unbounded) when the prefix is empty or all-0xff.
func prefixEnd(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

// --- Txn ---

func (f *fakeEtcd) Txn(ctx context.Context) clientv3.Txn { return &fakeTxn{f: f} }

type fakeTxn struct {
	f       *fakeEtcd
	cmps    []clientv3.Cmp
	thenOps []clientv3.Op
	elseOps []clientv3.Op
}

func (t *fakeTxn) If(cs ...clientv3.Cmp) clientv3.Txn { t.cmps = append(t.cmps, cs...); return t }
func (t *fakeTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.thenOps = append(t.thenOps, ops...)
	return t
}
func (t *fakeTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.elseOps = append(t.elseOps, ops...)
	return t
}

func (t *fakeTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.f.errTxn != nil {
		return nil, t.f.errTxn
	}
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	ok := true
	for _, c := range t.cmps {
		if !t.f.evalCmpLocked(c) {
			ok = false
			break
		}
	}
	ops := t.thenOps
	if !ok {
		ops = t.elseOps
	}
	resp := &clientv3.TxnResponse{Succeeded: ok}
	for _, op := range ops {
		resp.Responses = append(resp.Responses, t.f.execOpLocked(op))
	}
	// Capture the header after the writes so its revision reflects them, as a
	// real etcd transaction does.
	resp.Header = t.f.header()
	return resp, nil
}

// evalCmpLocked evaluates one comparison against the store.
func (f *fakeEtcd) evalCmpLocked(c clientv3.Cmp) bool {
	pc := pb.Compare(c)
	kv := f.store[string(pc.Key)]
	switch pc.Target {
	case pb.Compare_VALUE:
		var have []byte
		if kv != nil {
			have = kv.Value
		}
		return cmpResult(bytes.Compare(have, pc.GetValue()), pc.Result)
	case pb.Compare_VERSION:
		return cmpResult(cmpInt64(version(kv), pc.GetVersion()), pc.Result)
	case pb.Compare_CREATE:
		return cmpResult(cmpInt64(createRev(kv), pc.GetCreateRevision()), pc.Result)
	case pb.Compare_MOD:
		return cmpResult(cmpInt64(modRev(kv), pc.GetModRevision()), pc.Result)
	default: // pb.Compare_LEASE
		return cmpResult(cmpInt64(lease(kv), pc.GetLease()), pc.Result)
	}
}

func version(kv *mvccpb.KeyValue) int64 {
	if kv == nil {
		return 0
	}
	return kv.Version
}
func createRev(kv *mvccpb.KeyValue) int64 {
	if kv == nil {
		return 0
	}
	return kv.CreateRevision
}
func modRev(kv *mvccpb.KeyValue) int64 {
	if kv == nil {
		return 0
	}
	return kv.ModRevision
}
func lease(kv *mvccpb.KeyValue) int64 {
	if kv == nil {
		return 0
	}
	return kv.Lease
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// cmpResult maps a three-way comparison to the boolean the compare result wants.
func cmpResult(sign int, result pb.Compare_CompareResult) bool {
	switch result {
	case pb.Compare_EQUAL:
		return sign == 0
	case pb.Compare_GREATER:
		return sign > 0
	case pb.Compare_LESS:
		return sign < 0
	default: // pb.Compare_NOT_EQUAL
		return sign != 0
	}
}

// execOpLocked runs one transaction operation and returns its response op.
func (f *fakeEtcd) execOpLocked(op clientv3.Op) *pb.ResponseOp {
	switch {
	case op.IsPut():
		prev := f.putLocked(string(op.KeyBytes()), string(op.ValueBytes()))
		return &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{
			ResponsePut: &pb.PutResponse{Header: f.header(), PrevKv: prev},
		}}
	case op.IsDelete():
		deleted, prevs := f.deleteLocked(op)
		return &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{
			ResponseDeleteRange: &pb.DeleteRangeResponse{Header: f.header(), Deleted: deleted, PrevKvs: prevs},
		}}
	default: // get
		matched := f.matchLocked(op)
		return &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{
			ResponseRange: &pb.RangeResponse{Header: f.header(), Kvs: matched, Count: int64(len(matched))},
		}}
	}
}

// --- Watch ---

type fakeWatcher struct {
	ch     chan clientv3.WatchResponse
	key    string
	end    string // "" means single-key match
	prefix bool
}

func (w *fakeWatcher) matches(key string) bool {
	if !w.prefix {
		return key == w.key
	}
	if key < w.key {
		return false
	}
	return w.end == "" || key < w.end
}

func (f *fakeEtcd) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	op := clientv3.OpGet(key, opts...)
	w := &fakeWatcher{ch: make(chan clientv3.WatchResponse, 64), key: key, prefix: op.IsOptsWithPrefix()}
	if w.prefix {
		w.end = prefixEnd(key)
	}
	f.mu.Lock()
	f.watchers = append(f.watchers, w)
	if f.watchErr {
		w.ch <- clientv3.WatchResponse{Header: *f.header(), Canceled: true}
	}
	f.mu.Unlock()
	go func() {
		<-ctx.Done()
		f.mu.Lock()
		defer f.mu.Unlock()
		for i, other := range f.watchers {
			if other == w {
				f.watchers = append(f.watchers[:i], f.watchers[i+1:]...)
				break
			}
		}
		close(w.ch)
	}()
	return w.ch
}

// notifyLocked delivers an event to every matching watcher.
func (f *fakeEtcd) notifyLocked(t mvccpb.Event_EventType, kv, prev *mvccpb.KeyValue) {
	ev := &mvccpb.Event{Type: t, Kv: kv, PrevKv: prev}
	for _, w := range f.watchers {
		if w.matches(string(kv.Key)) {
			w.ch <- clientv3.WatchResponse{
				Header: *f.header(),
				Events: []*clientv3.Event{(*clientv3.Event)(ev)},
			}
		}
	}
}

func (f *fakeEtcd) RequestProgress(ctx context.Context) error { return nil }

// --- Lease ---

func (f *fakeEtcd) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errGrant != nil {
		return nil, f.errGrant
	}
	f.nextLease++
	return &clientv3.LeaseGrantResponse{ResponseHeader: f.header(), ID: LeaseID(f.nextLease), TTL: ttl}, nil
}

func (f *fakeEtcd) Revoke(ctx context.Context, id LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errRevoke != nil {
		return nil, f.errRevoke
	}
	return &clientv3.LeaseRevokeResponse{Header: f.header()}, nil
}

func (f *fakeEtcd) KeepAliveOnce(ctx context.Context, id LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errKA != nil {
		return nil, f.errKA
	}
	return &clientv3.LeaseKeepAliveResponse{ResponseHeader: f.header(), ID: id, TTL: 42}, nil
}

func (f *fakeEtcd) TimeToLive(ctx context.Context, id LeaseID, opts ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errTTL != nil {
		return nil, f.errTTL
	}
	return &clientv3.LeaseTimeToLiveResponse{
		ResponseHeader: f.header(),
		ID:             id,
		TTL:            7,
		GrantedTTL:     f.ttlGrant,
		Keys:           f.ttlKeys,
	}, nil
}

// --- Maintenance / Cluster ---

func (f *fakeEtcd) MemberList(ctx context.Context, opts ...clientv3.OpOption) (*clientv3.MemberListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errMember != nil {
		return nil, f.errMember
	}
	return &clientv3.MemberListResponse{Header: f.header(), Members: f.members}, nil
}

func (f *fakeEtcd) Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errStatus != nil {
		return nil, f.errStatus
	}
	s := f.status
	if s == nil {
		s = &pb.StatusResponse{}
	}
	s.Header = f.header()
	return (*clientv3.StatusResponse)(s), nil
}

func (f *fakeEtcd) Close() error { return f.errClose }
