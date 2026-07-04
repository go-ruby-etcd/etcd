// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EventType is the kind of change a watch delivers.
type EventType int

const (
	// EventPut is a create or update.
	EventPut EventType = iota
	// EventDelete is a deletion (or lease expiry).
	EventDelete
)

// String renders the event type as the gem does ("PUT" / "DELETE").
func (t EventType) String() string {
	if t == EventDelete {
		return "DELETE"
	}
	return "PUT"
}

// Event is a single watch event: the change kind and the affected key-value,
// with the previous value when the watch requested it.
type Event struct {
	Type   EventType
	Kv     KeyValue
	PrevKv *KeyValue
}

// WatchResult is one batch of watch events, mirroring a clientv3 WatchResponse.
// A batch with Err set is terminal.
type WatchResult struct {
	Header          Header
	Events          []Event
	Canceled        bool
	Created         bool
	CompactRevision int64
	Err             error
}

// Watch watches key (or a prefix/range when a range option is supplied) and
// returns a channel of event batches. The channel closes when ctx is cancelled
// or the watch is otherwise terminated. It mirrors the gem's #watch.
func (c *Client) Watch(ctx context.Context, key string, opts ...OpOption) <-chan WatchResult {
	wch := c.t.Watch(ctx, key, opts...)
	out := make(chan WatchResult)
	go func() {
		defer close(out)
		for wr := range wch {
			select {
			case out <- toWatchResult(wr):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// WatchBlock watches key and invokes fn with each non-empty batch of events,
// mirroring the etcdv3 gem's block form conn.watch(key) { |events| ... }. It
// blocks until the watch ends (ctx cancelled) or a terminal error arrives,
// returning that error (nil on a clean, ctx-driven stop).
func (c *Client) WatchBlock(ctx context.Context, fn func([]Event), key string, opts ...OpOption) error {
	for r := range c.Watch(ctx, key, opts...) {
		if r.Err != nil {
			return r.Err
		}
		if len(r.Events) > 0 {
			fn(r.Events)
		}
	}
	return nil
}

// toWatchResult maps a clientv3 WatchResponse to WatchResult.
func toWatchResult(wr clientv3.WatchResponse) WatchResult {
	r := WatchResult{
		Header:          toHeader(&wr.Header),
		Canceled:        wr.Canceled,
		Created:         wr.Created,
		CompactRevision: wr.CompactRevision,
		Err:             mapError(wr.Err()),
	}
	if len(wr.Events) > 0 {
		r.Events = make([]Event, len(wr.Events))
		for i, ev := range wr.Events {
			r.Events[i] = toEvent(ev)
		}
	}
	return r
}

// toEvent maps a clientv3 watch event to Event.
func toEvent(ev *clientv3.Event) Event {
	t := EventPut
	if ev.Type == mvccpb.DELETE {
		t = EventDelete
	}
	e := Event{Type: t, Kv: toKeyValue(ev.Kv)}
	e.PrevKv = toKeyValuePtr(ev.PrevKv)
	return e
}
