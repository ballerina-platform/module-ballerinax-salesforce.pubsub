// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.

import ballerina/grpc;

# Returns the canonical gRPC status name for a transport failure, when the
# error carries one of `grpc`'s distinct status types; `()` for a locally
# constructed error (for example a normal stream closure) that carries no
# transport status.
#
# + cause - a Subscribe stream failure
# + return - the gRPC status name, or `()` when the error has no gRPC status
public isolated function grpcStatusNameOf(error cause) returns string? {
    if cause is grpc:CancelledError {
        return "CANCELLED";
    } else if cause is grpc:UnKnownError {
        return "UNKNOWN";
    } else if cause is grpc:InvalidArgumentError {
        return "INVALID_ARGUMENT";
    } else if cause is grpc:DeadlineExceededError {
        return "DEADLINE_EXCEEDED";
    } else if cause is grpc:NotFoundError {
        return "NOT_FOUND";
    } else if cause is grpc:AlreadyExistsError {
        return "ALREADY_EXISTS";
    } else if cause is grpc:PermissionDeniedError {
        return "PERMISSION_DENIED";
    } else if cause is grpc:UnauthenticatedError {
        return "UNAUTHENTICATED";
    } else if cause is grpc:ResourceExhaustedError {
        return "RESOURCE_EXHAUSTED";
    } else if cause is grpc:FailedPreconditionError {
        return "FAILED_PRECONDITION";
    } else if cause is grpc:AbortedError {
        return "ABORTED";
    } else if cause is grpc:OutOfRangeError {
        return "OUT_OF_RANGE";
    } else if cause is grpc:UnimplementedError {
        return "UNIMPLEMENTED";
    } else if cause is grpc:UnavailableError {
        return "UNAVAILABLE";
    } else if cause is grpc:DataLossError {
        return "DATA_LOSS";
    } else if cause is grpc:InternalError {
        return "INTERNAL";
    }
    return ();
}

# Returns whether a transport status represents a temporary subscription
# failure. Handler errors are retried separately; malformed payloads, invalid
# requests, and authorization failures remain terminal.
#
# + status - gRPC status code name supplied by the transport layer
# + return - true when a closed subscription stream can be retried
public isolated function isReconnectableFailure(string status) returns boolean {
    string normalized = status.toUpperAscii();
    return normalized == "UNAVAILABLE" || normalized == "DEADLINE_EXCEEDED" ||
        normalized == "RESOURCE_EXHAUSTED" || normalized == "ABORTED";
}

# Classifies a stream receive failure without exposing transport records in the
# Listener API. A completed stream is reconnectable; ordinary authorization and
# malformed-request errors remain terminal.
#
# + streamError - receive or completion failure from the stream transport
# + return - true only for normal closure or known transient statuses
isolated function isReconnectableStreamError(error streamError) returns boolean {
    string? status = grpcStatusNameOf(streamError);
    if status is string && isReconnectableFailure(status) {
        return true;
    }
    string detail = streamError.message().toUpperAscii();
    return detail.includes("STREAM CLOSED") || detail.includes("UNAVAILABLE") || detail.includes("DEADLINE_EXCEEDED") ||
        detail.includes("RESOURCE_EXHAUSTED") || detail.includes("ABORTED");
}

# Classifies a reconnectable failure as affecting the shared gRPC channel
# rather than only its own Subscribe stream. `UNAVAILABLE` indicates the
# transport connection itself is down; other reconnectable statuses (a normal
# stream close, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`) are
# per-RPC and leave a healthy channel able to open a replacement stream
# directly.
#
# + streamError - receive or completion failure from the stream transport
# + return - true only when the whole channel, not just this stream, should be recreated
isolated function isChannelLevelFailure(error streamError) returns boolean {
    return grpcStatusNameOf(streamError) == "UNAVAILABLE" || streamError.message().toUpperAscii().includes("UNAVAILABLE");
}

# Classifies a Subscribe failure as Salesforce rejecting the supplied replay
# ID as invalid or past its retention window, distinct from an ordinary
# reconnectable transport failure. Recovery must restart the stream at the
# configured `expiredReplayRecovery` position rather than reusing the stale
# cursor or treating it as a normal stream-level reconnect.
#
# + streamError - receive or completion failure from the stream transport
# + return - true only when Salesforce rejected the supplied replay position
isolated function isInvalidReplayError(error streamError) returns boolean {
    return streamError.message().toUpperAscii().includes("REPLAY");
}

# Returns whether the configured retry budget has another retry after the
# initial attempt. `retryNumber` is one-based.
#
# + policy - retry timing and attempt budget
# + retryNumber - one-based retry number after the initial attempt
# + return - true when the retry number is within the configured budget
public isolated function canRetry(RetryPolicy policy, int retryNumber) returns boolean {
    return retryNumber > 0 && policy.maxRetries >= retryNumber;
}

# Calculates bounded exponential backoff for a one-based retry number.
#
# + policy - retry timing and attempt budget
# + retryNumber - one-based retry number after the initial attempt
# + return - delay in seconds, or an error for an invalid policy or retry number
public isolated function retryDelay(RetryPolicy policy, int retryNumber) returns decimal|error {
    check validateRetryPolicy(policy);
    if retryNumber <= 0 {
        return error("retry number must be greater than zero");
    }
    decimal delay = policy.initialDelay;
    int remainingDoublings = retryNumber - 1;
    while remainingDoublings > 0 && delay < policy.maxDelay {
        delay *= 2;
        remainingDoublings -= 1;
    }
    return delay > policy.maxDelay ? policy.maxDelay : delay;
}

isolated function validateRetryPolicy(RetryPolicy policy) returns error? {
    if policy.maxRetries < 0 || policy.initialDelay <= <decimal>0 || policy.maxDelay < policy.initialDelay {
        return error("invalid retry policy");
    }
}
