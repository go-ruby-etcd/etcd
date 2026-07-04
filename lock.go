// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"context"
	"fmt"
)

// Lock is a held distributed lock: the key that represents ownership and the
// lease that keeps it alive. Release it with [Client.Unlock].
type Lock struct {
	// Key is the lock-ownership key this holder created.
	Key string
	// Lease is the lease attached to Key; revoking it also frees the lock.
	Lease LeaseID
	// rev is the store revision at which Key was created, used to order waiters.
	rev int64
}

// Lock acquires a distributed lock named name, backed by a lease of ttl seconds,
// and blocks until it is held (or ctx is cancelled). It mirrors the etcdv3 gem's
// #lock and implements etcd's concurrency recipe: each caller writes a unique,
// lease-bound key under name/, and the caller whose key has the lowest creation
// revision owns the lock; later waiters watch the key just ahead of them until
// it is deleted.
func (c *Client) Lock(ctx context.Context, name string, ttl int64) (*Lock, error) {
	gr, err := c.LeaseGrant(ctx, ttl)
	if err != nil {
		return nil, err
	}
	pfx := name + "/"
	myKey := pfx + fmt.Sprintf("%x", int64(gr.ID))

	tr, err := c.Transaction(ctx, func(t *Txn) {
		t.If(Compare(CreateRevision(myKey), "=", 0)).
			Then(OpPut(myKey, "", WithLease(gr.ID))).
			Else(OpGet(myKey))
	})
	if err != nil {
		// Best-effort release of the lease we granted before failing.
		_, _ = c.LeaseRevoke(ctx, gr.ID)
		return nil, err
	}
	myRev := tr.Header.Revision

	for {
		resp, err := c.Get(ctx, pfx, WithPrefix())
		if err != nil {
			_, _ = c.LeaseRevoke(ctx, gr.ID)
			return nil, err
		}
		prior := priorKey(resp.Kvs, myRev)
		if prior == nil {
			// No key precedes ours: we own the lock.
			return &Lock{Key: myKey, Lease: gr.ID, rev: myRev}, nil
		}
		if err := c.waitDelete(ctx, prior.Key); err != nil {
			_, _ = c.LeaseRevoke(ctx, gr.ID)
			return nil, err
		}
	}
}

// Unlock releases a held lock: it deletes the ownership key and revokes its
// lease. It mirrors the gem's #unlock.
func (c *Client) Unlock(ctx context.Context, lk *Lock) error {
	if _, err := c.Del(ctx, lk.Key); err != nil {
		return err
	}
	if _, err := c.LeaseRevoke(ctx, lk.Lease); err != nil {
		return err
	}
	return nil
}

// priorKey returns the key-value with the greatest creation revision strictly
// below myRev, i.e. the waiter immediately ahead of us, or nil when none exists
// (meaning we hold the lowest revision and own the lock).
func priorKey(kvs []KeyValue, myRev int64) *KeyValue {
	var best *KeyValue
	for i := range kvs {
		kv := &kvs[i]
		if kv.CreateRevision < myRev {
			if best == nil || kv.CreateRevision > best.CreateRevision {
				best = kv
			}
		}
	}
	return best
}

// waitDelete blocks until key is deleted, returning nil on deletion, a watch
// error if the watch fails, or ctx.Err() if the watch ends without a deletion.
func (c *Client) waitDelete(ctx context.Context, key string) error {
	for wr := range c.Watch(ctx, key) {
		if wr.Err != nil {
			return wr.Err
		}
		for _, ev := range wr.Events {
			if ev.Type == EventDelete {
				return nil
			}
		}
	}
	return ctx.Err()
}
