// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// OpOption is a per-operation option (prefix, range, lease, ...). It aliases
// the clientv3 option type so options compose across this package and the
// underlying client.
type OpOption = clientv3.OpOption

// The gem-flavoured option constructors. Each mirrors an etcdv3 keyword and
// maps onto the corresponding clientv3 option.

// WithPrefix makes the operation act on every key sharing the given key as a
// prefix (the gem's range/prefix reads and deletes).
func WithPrefix() OpOption { return clientv3.WithPrefix() }

// WithRange makes the operation act on the half-open range [key, end); mirrors
// the gem's range_end keyword.
func WithRange(end string) OpOption { return clientv3.WithRange(end) }

// WithFromKey makes the operation act on all keys >= key.
func WithFromKey() OpOption { return clientv3.WithFromKey() }

// WithLease attaches a lease to a Put; mirrors the gem's lease keyword.
func WithLease(id LeaseID) OpOption { return clientv3.WithLease(id) }

// WithPrevKV asks Put/Del to return the previous key-value; mirrors prev_kv.
func WithPrevKV() OpOption { return clientv3.WithPrevKV() }

// WithLimit caps the number of keys a range read returns.
func WithLimit(n int64) OpOption { return clientv3.WithLimit(n) }

// WithRevision reads keys as of the given store revision.
func WithRevision(rev int64) OpOption { return clientv3.WithRev(rev) }

// WithCountOnly asks a read to return only the count of matching keys.
func WithCountOnly() OpOption { return clientv3.WithCountOnly() }

// WithKeysOnly asks a read to return keys without their values.
func WithKeysOnly() OpOption { return clientv3.WithKeysOnly() }

// WithSerializable permits a serializable (non-linearizable) read.
func WithSerializable() OpOption { return clientv3.WithSerializable() }

// Header is the response header etcd returns with every reply.
type Header struct {
	ClusterID uint64
	MemberID  uint64
	Revision  int64
	RaftTerm  uint64
}

// KeyValue mirrors the gem's key-value object (etcd's mvccpb.KeyValue).
type KeyValue struct {
	Key            string
	Value          string
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Lease          int64
}

// GetResult is the reply to Get: the matched key-values plus range metadata.
// It mirrors the gem's range response (kvs, count, more).
type GetResult struct {
	Header Header
	Kvs    []KeyValue
	More   bool
	Count  int64
}

// Kvs is a convenience alias for a slice of KeyValue, echoing the gem's naming.
type Kvs = []KeyValue

// First returns the first key-value, or nil when the result is empty. It mirrors
// the common gem idiom of reading response.kvs.first.
func (r *GetResult) First() *KeyValue {
	if len(r.Kvs) == 0 {
		return nil
	}
	return &r.Kvs[0]
}

// PutResult is the reply to Put; PrevKv is set when the previous value was
// requested (or surfaced by the backend).
type PutResult struct {
	Header Header
	PrevKv *KeyValue
}

// DelResult is the reply to Del: how many keys were deleted and, when
// requested, their previous values.
type DelResult struct {
	Header  Header
	Deleted int64
	PrevKvs []KeyValue
}

// Get retrieves key, or a range/prefix of keys when a range option is supplied.
// It mirrors the gem's #get.
func (c *Client) Get(ctx context.Context, key string, opts ...OpOption) (*GetResult, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}
	resp, err := c.t.Get(ctx, key, opts...)
	if err != nil {
		return nil, mapError(err)
	}
	return toGetResult(resp), nil
}

// Put stores value at key. It mirrors the gem's #put and honours WithLease and
// WithPrevKV.
func (c *Client) Put(ctx context.Context, key, value string, opts ...OpOption) (*PutResult, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}
	resp, err := c.t.Put(ctx, key, value, opts...)
	if err != nil {
		return nil, mapError(err)
	}
	return &PutResult{Header: toHeader(resp.Header), PrevKv: toKeyValuePtr(resp.PrevKv)}, nil
}

// Del deletes key, or a range/prefix when a range option is supplied. It mirrors
// the gem's #del.
func (c *Client) Del(ctx context.Context, key string, opts ...OpOption) (*DelResult, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}
	resp, err := c.t.Delete(ctx, key, opts...)
	if err != nil {
		return nil, mapError(err)
	}
	return &DelResult{
		Header:  toHeader(resp.Header),
		Deleted: resp.Deleted,
		PrevKvs: toKeyValues(resp.PrevKvs),
	}, nil
}

// Exists reports whether key is present. It mirrors the gem's #exists? and uses
// a count-only read so it transfers no values.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}
	resp, err := c.t.Get(ctx, key, clientv3.WithCountOnly())
	if err != nil {
		return false, mapError(err)
	}
	return resp.Count > 0, nil
}

// toHeader maps a protobuf response header to Header. A nil header (some backends
// omit it) maps to the zero Header.
func toHeader(h *pb.ResponseHeader) Header {
	if h == nil {
		return Header{}
	}
	return Header{
		ClusterID: h.ClusterId,
		MemberID:  h.MemberId,
		Revision:  h.Revision,
		RaftTerm:  h.RaftTerm,
	}
}

// toKeyValue maps a protobuf key-value to KeyValue.
func toKeyValue(kv *mvccpb.KeyValue) KeyValue {
	return KeyValue{
		Key:            string(kv.Key),
		Value:          string(kv.Value),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Lease:          kv.Lease,
	}
}

// toKeyValuePtr maps a possibly-nil protobuf key-value to a *KeyValue.
func toKeyValuePtr(kv *mvccpb.KeyValue) *KeyValue {
	if kv == nil {
		return nil
	}
	out := toKeyValue(kv)
	return &out
}

// toKeyValues maps a slice of protobuf key-values.
func toKeyValues(kvs []*mvccpb.KeyValue) []KeyValue {
	if len(kvs) == 0 {
		return nil
	}
	out := make([]KeyValue, len(kvs))
	for i, kv := range kvs {
		out[i] = toKeyValue(kv)
	}
	return out
}

// toGetResult maps a GetResponse to GetResult.
func toGetResult(resp *clientv3.GetResponse) *GetResult {
	return &GetResult{
		Header: toHeader(resp.Header),
		Kvs:    toKeyValues(resp.Kvs),
		More:   resp.More,
		Count:  resp.Count,
	}
}
