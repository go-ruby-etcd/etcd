// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"context"
	"crypto/tls"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// LeaseID identifies a lease. It mirrors clientv3.LeaseID (an int64), which is
// the "ID" the etcdv3 gem returns from lease_grant.
type LeaseID = clientv3.LeaseID

// transport is the host seam. Its method set is satisfied directly by
// *clientv3.Client, so [New] injects a real client with no adapter, while tests
// inject a deterministic in-memory implementation. Keeping the seam in terms of
// the clientv3 types means the Client's request-building and response-mapping
// logic is exercised identically whether the backend is real or fake, which is
// what lets the deterministic suite reach 100% coverage without a live server.
type transport interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Txn(ctx context.Context) clientv3.Txn
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	Revoke(ctx context.Context, id LeaseID) (*clientv3.LeaseRevokeResponse, error)
	KeepAliveOnce(ctx context.Context, id LeaseID) (*clientv3.LeaseKeepAliveResponse, error)
	TimeToLive(ctx context.Context, id LeaseID, opts ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error)
	MemberList(ctx context.Context, opts ...clientv3.OpOption) (*clientv3.MemberListResponse, error)
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
	Close() error
}

// Config configures a [Client]. The fields mirror the keywords the etcdv3 gem's
// Etcdv3.new accepts that affect connecting to the cluster.
type Config struct {
	// Endpoints is the list of etcd endpoints to connect to, e.g.
	// []string{"127.0.0.1:2379"}. A leading "http://" or "https://" scheme is
	// accepted and normalised away, matching the gem which takes a URL.
	Endpoints []string
	// DialTimeout bounds the initial connection. Zero uses the client default.
	DialTimeout time.Duration
	// Username and Password enable authenticated connections when both are set.
	Username string
	Password string
	// TLS, when set, is applied to the underlying client for https endpoints.
	// It is optional; nil means plaintext (or scheme-derived) transport.
	TLS *tls.Config
}

// Client is an etcd v3 client bound to a transport seam. It mirrors the etcdv3
// gem's connection object: it builds requests, drives them over the transport,
// and maps responses to the gem's result and error model. Construct one with
// [New]; the zero value is not usable.
type Client struct {
	t transport
}

// Connection is the etcdv3 gem's name for the object Etcdv3.new returns. It is
// an alias for [Client].
type Connection = Client

// New connects to etcd using cfg and returns a [Client]. It builds a
// *clientv3.Client, which dials lazily, so New returns without a round trip and
// fails only on a malformed configuration (for example no endpoints).
func New(cfg Config) (*Client, error) {
	cc := clientv3.Config{
		Endpoints:   normalizeEndpoints(cfg.Endpoints),
		DialTimeout: cfg.DialTimeout,
		Username:    cfg.Username,
		Password:    cfg.Password,
		TLS:         cfg.TLS,
	}
	c, err := clientv3.New(cc)
	if err != nil {
		return nil, mapError(err)
	}
	return &Client{t: c}, nil
}

// newWithTransport wraps an arbitrary transport. It is the seam tests use to
// drive the Client against a deterministic in-memory backend.
func newWithTransport(t transport) *Client { return &Client{t: t} }

// Close releases the client's resources, mirroring the gem connection's close.
func (c *Client) Close() error { return c.t.Close() }

// normalizeEndpoints strips a leading http:// or https:// scheme from each
// endpoint, since the gem accepts URLs while clientv3 wants host:port. Empty
// input yields nil so clientv3.New reports the missing-endpoints error.
func normalizeEndpoints(eps []string) []string {
	if len(eps) == 0 {
		return nil
	}
	out := make([]string, len(eps))
	for i, ep := range eps {
		out[i] = stripScheme(ep)
	}
	return out
}

// stripScheme removes a leading http:// or https:// from ep.
func stripScheme(ep string) string {
	const https = "https://"
	const http = "http://"
	switch {
	case len(ep) >= len(https) && ep[:len(https)] == https:
		return ep[len(https):]
	case len(ep) >= len(http) && ep[:len(http)] == http:
		return ep[len(http):]
	default:
		return ep
	}
}
