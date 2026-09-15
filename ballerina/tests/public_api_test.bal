import ballerina/http;
import ballerina/test;

// This fails if default subscription behavior starts from an earlier replay
// position or exposes an unbounded flow-control setting.
@test:Config {}
function testSubscriptionConfigHasSafeV1Defaults() {
    SubscriptionConfig config = {};

    test:assertEquals(config.initialReplay, LATEST);
    test:assertEquals(config.expiredReplayRecovery, EARLIEST);
    test:assertEquals(config.bufferSize, 10);
}

// This fails if a Listener's shared subscriber identity silently changes,
// which would move every attached topic's checkpoint to a new key on restart.
@test:Config {}
function testListenerConfigDefaultsLogicalSubscriptionName() {
    ListenerConfig config = {
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        }
    };

    test:assertEquals(config.logicalSubscriptionName, "default");
}

// This fails if public events lose duplicate binary headers or require an event
// ID for CDC and other event families that do not provide one.
@test:Config {}
function testEventEnvelopePreservesOptionalIdAndRepeatedBinaryHeaders() {
    Event event = {
        topic: "/data/AccountChangeEvent",
        replayId: [1, 2],
        schemaId: "schema-1",
        headers: [{key: "trace", value: [10]}, {key: "trace", value: [11]}],
        payload: {"Name": "Acme"}
    };

    test:assertEquals(event.eventId, ());
    test:assertEquals(event.headers.length(), 2);
    test:assertEquals(event.headers[1].value, [11]);
    test:assertTrue(event.payload.toString().includes("Acme"));
}
