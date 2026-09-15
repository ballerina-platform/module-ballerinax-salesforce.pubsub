import ballerina/test;

// This fails if terminal listener diagnostics expose credential-like detail
// from an underlying transport error.
@test:Config {}
function testListenerErrorRedactsSensitiveTransportDetail() {
    ListenerError diagnostic = listenerErrorFor(
        "Subscribe", "/event/Order__e", error("access token secret-token-value rejected"), "UNAUTHENTICATED", (), "rpc-1"
    );

    test:assertEquals(diagnostic.operation, "Subscribe");
    test:assertEquals(diagnostic.rpcId, "rpc-1");
    test:assertFalse(diagnostic.cause.message().includes("secret-token-value"));
}
