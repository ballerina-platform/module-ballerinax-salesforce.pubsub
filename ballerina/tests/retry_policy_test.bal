import ballerina/grpc;
import ballerina/test;

// These classifications are connector-owned: malformed requests and normal
// permission failures must be terminal, while temporary transport failures can
// reconnect with the configured bounded retry policy.
@test:Config {}
function testRetryClassifierSeparatesTransientAndTerminalFailures() {
    test:assertTrue(isReconnectableFailure("UNAVAILABLE"));
    test:assertTrue(isReconnectableFailure("deadline_exceeded"));
    test:assertFalse(isReconnectableFailure("PERMISSION_DENIED"));
    test:assertFalse(isReconnectableFailure("INVALID_ARGUMENT"));
}

// The first retry uses initialDelay, subsequent retries double until maxDelay,
// and the initial attempt itself is never counted as a retry.
@test:Config {}
function testRetryDelayIsBoundedAndCountsOnlyRetries() returns error? {
    RetryPolicy policy = {maxRetries: 3, initialDelay: 2, maxDelay: 5};

    test:assertEquals(check retryDelay(policy, 1), <decimal>2);
    test:assertEquals(check retryDelay(policy, 2), <decimal>4);
    test:assertEquals(check retryDelay(policy, 3), <decimal>5);
    test:assertTrue(canRetry(policy, 3));
    test:assertFalse(canRetry(policy, 4));
}

@test:Config {}
function testRetryDelayRejectsInvalidPolicies() {
    RetryPolicy invalid = {maxRetries: -1, initialDelay: 0, maxDelay: 1};
    test:assertTrue(retryDelay(invalid, 1) is error);
}

// A normal server-side stream completion has no gRPC status, but it is a
// reconnectable condition distinct from malformed or permission failures.
@test:Config {}
function testReconnectClassifierIncludesNormalStreamClosure() {
    test:assertTrue(isReconnectableStreamError(error("Subscribe stream closed")));
    test:assertTrue(isReconnectableStreamError(error("UNAVAILABLE: transport lost")));
    test:assertFalse(isReconnectableStreamError(error("PERMISSION_DENIED")));
}

// Only a broken transport connection (UNAVAILABLE) should force recreating
// the shared channel; a normal stream close or other per-RPC reconnectable
// statuses leave a healthy channel able to open a replacement stream directly.
// The transport surfaces gRPC failures as `grpc`'s distinct status error
// types (confirmed against the local fixture, not just assumed), so status
// extraction must key on runtime type rather than message text; a locally
// constructed error carries no such type.
@test:Config {}
function testGrpcStatusNameOfReadsTheDistinctTransportType() {
    test:assertEquals(grpcStatusNameOf(error grpc:UnavailableError("down")), "UNAVAILABLE");
    test:assertEquals(grpcStatusNameOf(error grpc:PermissionDeniedError("denied")), "PERMISSION_DENIED");
    test:assertEquals(grpcStatusNameOf(error grpc:FailedPreconditionError("bad cursor")), "FAILED_PRECONDITION");
    test:assertEquals(grpcStatusNameOf(error("Subscribe stream closed")), ());
}

@test:Config {}
function testChannelLevelFailureClassifierIsolatesTransportLoss() {
    test:assertTrue(isChannelLevelFailure(error("UNAVAILABLE: transport lost")));
    test:assertFalse(isChannelLevelFailure(error("Subscribe stream closed")));
    test:assertFalse(isChannelLevelFailure(error("DEADLINE_EXCEEDED")));
    test:assertFalse(isChannelLevelFailure(error("RESOURCE_EXHAUSTED")));
    test:assertFalse(isChannelLevelFailure(error("ABORTED")));
}
