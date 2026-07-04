// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// LeaseGrantResult is the reply to LeaseGrant. ID is the granted lease id (the
// gem's lease['ID']); Error is set when the grant partially failed.
type LeaseGrantResult struct {
	Header Header
	ID     LeaseID
	TTL    int64
	Error  string
}

// LeaseKeepAliveResult is the reply to a single keep-alive.
type LeaseKeepAliveResult struct {
	Header Header
	ID     LeaseID
	TTL    int64
}

// LeaseRevokeResult is the reply to LeaseRevoke.
type LeaseRevokeResult struct {
	Header Header
}

// LeaseTTLResult is the reply to LeaseTTL: the remaining and granted TTLs and,
// when requested, the keys attached to the lease.
type LeaseTTLResult struct {
	Header     Header
	ID         LeaseID
	TTL        int64
	GrantedTTL int64
	Keys       []string
}

// LeaseGrant grants a lease that expires after ttl seconds. It mirrors the gem's
// #lease_grant.
func (c *Client) LeaseGrant(ctx context.Context, ttl int64) (*LeaseGrantResult, error) {
	resp, err := c.t.Grant(ctx, ttl)
	if err != nil {
		return nil, mapError(err)
	}
	return &LeaseGrantResult{
		Header: toHeader(resp.ResponseHeader),
		ID:     resp.ID,
		TTL:    resp.TTL,
		Error:  resp.Error,
	}, nil
}

// LeaseKeepAliveOnce renews a lease once. It mirrors the gem's
// #lease_keep_alive_once (the single-shot keep-alive).
func (c *Client) LeaseKeepAliveOnce(ctx context.Context, id LeaseID) (*LeaseKeepAliveResult, error) {
	resp, err := c.t.KeepAliveOnce(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &LeaseKeepAliveResult{
		Header: toHeader(resp.ResponseHeader),
		ID:     resp.ID,
		TTL:    resp.TTL,
	}, nil
}

// LeaseRevoke revokes a lease, deleting every key attached to it. It mirrors the
// gem's #lease_revoke.
func (c *Client) LeaseRevoke(ctx context.Context, id LeaseID) (*LeaseRevokeResult, error) {
	resp, err := c.t.Revoke(ctx, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &LeaseRevokeResult{Header: toHeader(resp.Header)}, nil
}

// LeaseTTL returns a lease's remaining TTL. When withKeys is true the result
// also lists the keys attached to the lease. It mirrors the gem's #lease_ttl.
func (c *Client) LeaseTTL(ctx context.Context, id LeaseID, withKeys bool) (*LeaseTTLResult, error) {
	var opts []clientv3.LeaseOption
	if withKeys {
		opts = append(opts, clientv3.WithAttachedKeys())
	}
	resp, err := c.t.TimeToLive(ctx, id, opts...)
	if err != nil {
		return nil, mapError(err)
	}
	out := &LeaseTTLResult{
		Header:     toHeader(resp.ResponseHeader),
		ID:         resp.ID,
		TTL:        resp.TTL,
		GrantedTTL: resp.GrantedTTL,
	}
	if len(resp.Keys) > 0 {
		out.Keys = make([]string, len(resp.Keys))
		for i, k := range resp.Keys {
			out.Keys[i] = string(k)
		}
	}
	return out, nil
}
