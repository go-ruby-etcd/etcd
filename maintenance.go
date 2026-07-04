// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import "context"

// Member describes one cluster member, mirroring the gem's member object.
type Member struct {
	ID         uint64
	Name       string
	PeerURLs   []string
	ClientURLs []string
	IsLearner  bool
}

// MembersResult is the reply to Members.
type MembersResult struct {
	Header  Header
	Members []Member
}

// StatusResult is the reply to Status: a member's version, database size and
// raft state. It mirrors the gem's #status.
type StatusResult struct {
	Header    Header
	Version   string
	DbSize    int64
	Leader    uint64
	RaftIndex uint64
	RaftTerm  uint64
	IsLearner bool
}

// Members lists the cluster's members. It mirrors the gem's #members.
func (c *Client) Members(ctx context.Context) (*MembersResult, error) {
	resp, err := c.t.MemberList(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	out := &MembersResult{Header: toHeader(resp.Header)}
	if len(resp.Members) > 0 {
		out.Members = make([]Member, len(resp.Members))
		for i, m := range resp.Members {
			out.Members[i] = Member{
				ID:         m.ID,
				Name:       m.Name,
				PeerURLs:   m.PeerURLs,
				ClientURLs: m.ClientURLs,
				IsLearner:  m.IsLearner,
			}
		}
	}
	return out, nil
}

// Status returns the status of the member serving endpoint. It mirrors the gem's
// #status.
func (c *Client) Status(ctx context.Context, endpoint string) (*StatusResult, error) {
	resp, err := c.t.Status(ctx, endpoint)
	if err != nil {
		return nil, mapError(err)
	}
	return &StatusResult{
		Header:    toHeader(resp.Header),
		Version:   resp.Version,
		DbSize:    resp.DbSize,
		Leader:    resp.Leader,
		RaftIndex: resp.RaftIndex,
		RaftTerm:  resp.RaftTerm,
		IsLearner: resp.IsLearner,
	}, nil
}
