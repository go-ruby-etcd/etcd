// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Cmp is a transaction comparison, built with [Compare] over one of [Value],
// [Version], [CreateRevision], [ModRevision] or [LeaseValue]. It aliases the
// clientv3 type so it drives the real transaction machinery directly.
type Cmp = clientv3.Cmp

// Op is a transaction operation, built with [OpGet], [OpPut] or [OpDelete].
type Op = clientv3.Op

// Value targets a key's value in a comparison (the gem's txn.value).
func Value(key string) Cmp { return clientv3.Value(key) }

// Version targets a key's version in a comparison.
func Version(key string) Cmp { return clientv3.Version(key) }

// CreateRevision targets a key's creation revision in a comparison.
func CreateRevision(key string) Cmp { return clientv3.CreateRevision(key) }

// ModRevision targets a key's modification revision in a comparison.
func ModRevision(key string) Cmp { return clientv3.ModRevision(key) }

// LeaseValue targets a key's attached lease in a comparison.
func LeaseValue(key string) Cmp { return clientv3.LeaseValue(key) }

// Compare finishes a comparison: op is one of "=", "!=", ">", "<" and v is the
// value to compare the target against. It mirrors the gem's comparison DSL.
func Compare(cmp Cmp, op string, v any) Cmp { return clientv3.Compare(cmp, op, v) }

// OpGet builds a get operation for a transaction branch.
func OpGet(key string, opts ...OpOption) Op { return clientv3.OpGet(key, opts...) }

// OpPut builds a put operation for a transaction branch.
func OpPut(key, val string, opts ...OpOption) Op { return clientv3.OpPut(key, val, opts...) }

// OpDelete builds a delete operation for a transaction branch.
func OpDelete(key string, opts ...OpOption) Op { return clientv3.OpDelete(key, opts...) }

// Txn accumulates a transaction's comparisons and branches. It is filled inside
// the callback passed to [Client.Transaction] and mirrors the gem's txn object
// whose compare / success / failure lists map to [Txn.If] / [Txn.Then] /
// [Txn.Else].
type Txn struct {
	cmps    []Cmp
	thenOps []Op
	elseOps []Op
}

// If adds comparisons; the success branch runs only if they all hold. It mirrors
// setting the gem's txn.compare.
func (t *Txn) If(cs ...Cmp) *Txn { t.cmps = append(t.cmps, cs...); return t }

// Then adds operations to the success branch (the gem's txn.success).
func (t *Txn) Then(ops ...Op) *Txn { t.thenOps = append(t.thenOps, ops...); return t }

// Else adds operations to the failure branch (the gem's txn.failure).
func (t *Txn) Else(ops ...Op) *Txn { t.elseOps = append(t.elseOps, ops...); return t }

// TxnOpResult is one operation's reply within a transaction; exactly one of the
// fields is set, matching the operation kind.
type TxnOpResult struct {
	Get *GetResult
	Put *PutResult
	Del *DelResult
}

// TxnResult is the reply to a transaction: whether the comparisons held and the
// per-operation replies of the branch that ran.
type TxnResult struct {
	Header    Header
	Succeeded bool
	Responses []TxnOpResult
}

// Transaction runs an etcd transaction. The callback fills a [Txn] with
// comparisons (If) and success/failure operations (Then/Else); the transaction
// is then committed atomically. It mirrors the gem's #transaction block.
func (c *Client) Transaction(ctx context.Context, build func(*Txn)) (*TxnResult, error) {
	t := &Txn{}
	build(t)
	resp, err := c.t.Txn(ctx).If(t.cmps...).Then(t.thenOps...).Else(t.elseOps...).Commit()
	if err != nil {
		return nil, mapError(err)
	}
	return toTxnResult(resp), nil
}

// toTxnResult maps a clientv3 TxnResponse to TxnResult.
func toTxnResult(resp *clientv3.TxnResponse) *TxnResult {
	out := &TxnResult{Header: toHeader(resp.Header), Succeeded: resp.Succeeded}
	for _, r := range resp.Responses {
		var o TxnOpResult
		switch {
		case r.GetResponseRange() != nil:
			o.Get = toGetResult((*clientv3.GetResponse)(r.GetResponseRange()))
		case r.GetResponsePut() != nil:
			pr := r.GetResponsePut()
			o.Put = &PutResult{Header: toHeader(pr.Header), PrevKv: toKeyValuePtr(pr.PrevKv)}
		case r.GetResponseDeleteRange() != nil:
			dr := r.GetResponseDeleteRange()
			o.Del = &DelResult{Header: toHeader(dr.Header), Deleted: dr.Deleted, PrevKvs: toKeyValues(dr.PrevKvs)}
		}
		out.Responses = append(out.Responses, o)
	}
	return out
}
