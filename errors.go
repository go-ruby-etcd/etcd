// Copyright (c) the go-ruby-etcd/etcd authors
//
// SPDX-License-Identifier: BSD-3-Clause

package etcd

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Error is the base of the etcdv3 error tree. It carries the gRPC status code
// etcd returned and mirrors the etcdv3 gem, whose error classes map one-to-one
// onto the gRPC status codes. Match a specific kind with [errors.Is] against
// one of the exported sentinels (for example [ErrNotFound]); the match is by
// status code, so a wrapped Error compares equal to its sentinel.
type Error struct {
	// Code is the gRPC status code etcd reported.
	Code codes.Code
	// Name is the etcdv3 error-class name for Code (e.g. "NotFound").
	Name string
	// Message is the human-readable detail from the status.
	Message string
	// cause is the underlying error, if any.
	cause error
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("etcd: %s (%s)", e.Name, e.Code)
	}
	return fmt.Sprintf("etcd: %s (%s): %s", e.Name, e.Code, e.Message)
}

// Unwrap exposes the underlying transport error for [errors.Unwrap].
func (e *Error) Unwrap() error { return e.cause }

// Is reports whether target is an *Error with the same status code, so the
// exported sentinels match any Error of the same kind regardless of message.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Code == e.Code
}

// The etcdv3 error tree: one sentinel per gRPC status code. Compare with
// errors.Is, e.g. errors.Is(err, etcd.ErrNotFound).
var (
	ErrCanceled           = &Error{Code: codes.Canceled, Name: "Canceled"}
	ErrUnknown            = &Error{Code: codes.Unknown, Name: "Unknown"}
	ErrInvalidArgument    = &Error{Code: codes.InvalidArgument, Name: "InvalidArgument"}
	ErrDeadlineExceeded   = &Error{Code: codes.DeadlineExceeded, Name: "DeadlineExceeded"}
	ErrNotFound           = &Error{Code: codes.NotFound, Name: "NotFound"}
	ErrAlreadyExists      = &Error{Code: codes.AlreadyExists, Name: "AlreadyExists"}
	ErrPermissionDenied   = &Error{Code: codes.PermissionDenied, Name: "PermissionDenied"}
	ErrResourceExhausted  = &Error{Code: codes.ResourceExhausted, Name: "ResourceExhausted"}
	ErrFailedPrecondition = &Error{Code: codes.FailedPrecondition, Name: "FailedPrecondition"}
	ErrAborted            = &Error{Code: codes.Aborted, Name: "Aborted"}
	ErrOutOfRange         = &Error{Code: codes.OutOfRange, Name: "OutOfRange"}
	ErrUnimplemented      = &Error{Code: codes.Unimplemented, Name: "Unimplemented"}
	ErrInternal           = &Error{Code: codes.Internal, Name: "Internal"}
	ErrUnavailable        = &Error{Code: codes.Unavailable, Name: "Unavailable"}
	ErrDataLoss           = &Error{Code: codes.DataLoss, Name: "DataLoss"}
	ErrUnauthenticated    = &Error{Code: codes.Unauthenticated, Name: "Unauthenticated"}
)

// codeName maps a gRPC status code to its etcdv3 error-class name.
var codeName = map[codes.Code]string{
	codes.Canceled:           "Canceled",
	codes.Unknown:            "Unknown",
	codes.InvalidArgument:    "InvalidArgument",
	codes.DeadlineExceeded:   "DeadlineExceeded",
	codes.NotFound:           "NotFound",
	codes.AlreadyExists:      "AlreadyExists",
	codes.PermissionDenied:   "PermissionDenied",
	codes.ResourceExhausted:  "ResourceExhausted",
	codes.FailedPrecondition: "FailedPrecondition",
	codes.Aborted:            "Aborted",
	codes.OutOfRange:         "OutOfRange",
	codes.Unimplemented:      "Unimplemented",
	codes.Internal:           "Internal",
	codes.Unavailable:        "Unavailable",
	codes.DataLoss:           "DataLoss",
	codes.Unauthenticated:    "Unauthenticated",
}

// mapError converts a transport error into an *Error, translating gRPC status
// codes into the etcdv3 error tree. A nil error maps to nil. An error that is
// already an *Error is returned unchanged.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var already *Error
	if errors.As(err, &already) {
		return already
	}
	st, _ := status.FromError(err)
	code := st.Code()
	name, ok := codeName[code]
	if !ok {
		name = "Unknown"
	}
	return &Error{Code: code, Name: name, Message: st.Message(), cause: err}
}

// ErrEmptyKey is returned by key operations when the key is empty; etcd rejects
// empty keys, and rejecting them locally avoids a needless round trip.
var ErrEmptyKey = &Error{Code: codes.InvalidArgument, Name: "InvalidArgument", Message: "empty key"}
