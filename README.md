<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-etcd/brand/main/social/go-ruby-etcd-etcd.png" alt="go-ruby-etcd/etcd" width="720"></p>

# etcd — go-ruby-etcd

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-etcd.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo), MRI-faithful reimplementation of the Ruby
[`etcdv3`](https://github.com/davissp14/etcdv3-ruby) gem's client surface for
etcd v3** — the ergonomics and result/error model of the gem's connection
object (`get` / `put` / `del` / `exists?`, `watch`, `lease_*`, `transaction`,
`lock` / `unlock`) layered over the official pure-Go etcd client.

It does **not** reimplement the etcd protocol. It consumes
[`go.etcd.io/etcd/client/v3`](https://pkg.go.dev/go.etcd.io/etcd/client/v3) as
its transport and maps the gem's API onto it, so a static, CGO=0 binary talks to
a real etcd cluster.

It is the etcd backend for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby), but is a
**standalone, reusable** module — a sibling of
[go-ruby-redis](https://github.com/go-ruby-redis/redis) and
[go-ruby-pg](https://github.com/go-ruby-pg/pg).

> **Transport is a host seam.** A [`Client`](client.go) drives an injected
> transport whose method set is satisfied directly by `*clientv3.Client` (no
> adapter). This makes every method's request-building and response-mapping
> logic testable against a **deterministic in-memory transport** — no external
> etcd, no cgo — so the suite holds **100% coverage on every arch under qemu**,
> mirroring the go-ruby-nats embedded-server-plus-deterministic-fallback split.
> A separate [live suite](live/) drives a real in-process embedded etcd for
> round-trip validation on native lanes.

## Features

- **KV** — `Get` (single, range via `WithRange`, prefix via `WithPrefix`,
  `WithFromKey`, `WithLimit`, `WithRevision`, `WithSerializable`, `WithKeysOnly`,
  `WithCountOnly`), `Put` (`WithLease`, `WithPrevKV`), `Del`, `Exists`.
- **Watch** — `Watch` (channel form) and `WatchBlock` (the gem's
  `watch(key){ |events| }` block form) over keys, prefixes and ranges, with
  `PUT` / `DELETE` events and previous values.
- **Lease** — `LeaseGrant`, `LeaseKeepAliveOnce`, `LeaseRevoke`, `LeaseTTL`
  (with attached keys).
- **Transaction** — `Transaction { If / Then / Else }` mapping the gem's
  compare / success / failure lists, with `Compare` over `Value` / `Version` /
  `CreateRevision` / `ModRevision` / `LeaseValue`.
- **Lock** — `Lock` / `Unlock`, a lease-backed mutex implementing etcd's
  concurrency recipe (lowest creation-revision owns; waiters watch their
  predecessor).
- **Maintenance** — `Members`, `Status`.
- **Errors** — an `Etcdv3`-style error tree (`Error` + one sentinel per gRPC
  status code) matchable with `errors.Is`.

## Usage

```go
c, err := etcd.New(etcd.Config{Endpoints: []string{"127.0.0.1:2379"}})
if err != nil {
	log.Fatal(err)
}
defer c.Close()

ctx := context.Background()
c.Put(ctx, "/service/a", "up", etcd.WithLease(lease.ID))
gr, _ := c.Get(ctx, "/service/", etcd.WithPrefix())
for _, kv := range gr.Kvs {
	fmt.Println(kv.Key, "=", kv.Value)
}

c.Transaction(ctx, func(t *etcd.Txn) {
	t.If(etcd.Compare(etcd.Value("/service/a"), "=", "up")).
		Then(etcd.OpPut("/service/a", "draining")).
		Else(etcd.OpGet("/service/a"))
})
```

## Ruby mapping

| etcdv3 gem                            | go-ruby-etcd/etcd                                   |
| ------------------------------------- | --------------------------------------------------- |
| `Etcdv3.new(endpoints: ...)`          | `etcd.New(etcd.Config{Endpoints: ...})`             |
| `conn.put('k', 'v')`                  | `c.Put(ctx, "k", "v")`                              |
| `conn.get('k', range_end: 'l')`       | `c.Get(ctx, "k", etcd.WithRange("l"))`              |
| `conn.del('k')` / `conn.exists?('k')` | `c.Del(ctx, "k")` / `c.Exists(ctx, "k")`            |
| `conn.watch('k') { \|evs\| ... }`      | `c.WatchBlock(ctx, fn, "k")` / `c.Watch(ctx, "k")`  |
| `conn.lease_grant(10)`                | `c.LeaseGrant(ctx, 10)`                             |
| `conn.transaction { \|t\| ... }`       | `c.Transaction(ctx, func(t *etcd.Txn){ ... })`      |
| `conn.lock('n', 10)` / `conn.unlock`  | `c.Lock(ctx, "n", 10)` / `c.Unlock(ctx, lk)`        |

## Tests & coverage

The default suite runs with `-race` and holds **100% statement coverage** on all
three host OSes and the six supported 64-bit architectures (amd64, arm64,
riscv64, loong64, ppc64le and big-endian s390x), driving the full client logic
against a deterministic in-memory transport with no external etcd:

```sh
go test -race -cover ./...
```

The [`live/`](live/) nested module validates real round-trip behaviour against
an in-process embedded etcd (native-only; kept out of the main module so its
large dependency tree never enters this go.mod):

```sh
cd live && go test ./...
```

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright (c) 2026, the go-ruby-etcd/etcd
authors.

## WebAssembly

Being pure Go (CGO=0), this library also compiles to **WebAssembly** — both
`GOOS=js GOARCH=wasm` (browser / Node.js) and `GOOS=wasip1 GOARCH=wasm` (WASI).
CI builds both targets on every push, alongside the six 64-bit native/qemu arches.

```sh
GOOS=js     GOARCH=wasm go build ./...   # browser / Node
GOOS=wasip1 GOARCH=wasm go build ./...   # WASI (wasmtime, wasmer, wasmedge, …)
```
