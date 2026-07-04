// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

// Package etcd is a pure-Go (CGO=0), MRI-faithful reimplementation of the Ruby
// etcdv3 gem's client surface for etcd v3.
//
// It does not reimplement the etcd protocol. It consumes the official pure-Go
// client go.etcd.io/etcd/client/v3 as its transport and layers the ergonomics
// and result/error model of the etcdv3 gem on top: Client.Get / Put / Del /
// Exists (single, range and prefix), Watch, lease grant / keep-alive / revoke /
// ttl, Transaction (compare / success / failure), and a lease-backed Lock /
// Unlock mirroring etcd's concurrency recipe. Maintenance (Members, Status) is
// exposed too.
//
// # Transport is a host seam
//
// A Client drives an injected [transport] whose method set is satisfied
// directly by *clientv3.Client (no adapter). This makes every method's logic
// testable against a deterministic in-memory transport with no external etcd
// and no cgo, so the suite reaches 100% coverage on every arch under qemu,
// mirroring the go-ruby-nats embedded-server-plus-deterministic-fallback split.
// A separate live suite (build tag "live") drives an in-process embedded etcd
// for real round-trip validation on native lanes.
//
// # Ruby mapping
//
//	conn = Etcdv3.new(endpoints: 'http://127.0.0.1:2379')  =>  etcd.New(etcd.Config{Endpoints: ...})
//	conn.put('k', 'v')                                     =>  c.Put(ctx, "k", "v")
//	conn.get('k')                                          =>  c.Get(ctx, "k")
//	conn.get('k', range_end: 'l')                          =>  c.Get(ctx, "k", etcd.WithRange("l"))
//	conn.del('k')                                          =>  c.Del(ctx, "k")
//	conn.exists?('k')                                      =>  c.Exists(ctx, "k")
//	conn.watch('k') { |events| ... }                       =>  c.WatchBlock(ctx, fn, "k") / c.Watch(ctx, "k")
//	conn.lease_grant(10)                                   =>  c.LeaseGrant(ctx, 10)
//	conn.transaction { |t| ... }                            =>  c.Transaction(ctx, func(t *etcd.Txn){ ... })
//	conn.lock('n', 10) / conn.unlock(key)                   =>  c.Lock(ctx, "n", 10) / c.Unlock(ctx, lk)
package etcd
