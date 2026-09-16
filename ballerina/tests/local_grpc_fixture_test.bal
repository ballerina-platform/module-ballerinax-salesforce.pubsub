// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/grpc;
import ballerina/http;
import ballerina/lang.runtime;
import ballerina/test;
import ballerina/time;
import ballerinax/salesforce.pubsub.internal as wire;

final int FIXTURE_PORT = 19091;
final string FIXTURE_TOPIC = "/event/Fixture__e";
final string FIXTURE_ERROR_TOPIC = "/event/FixtureError__e";
final string FIXTURE_RECONNECT_TOPIC = "/event/FixtureReconnect__e";
final string FIXTURE_RECONNECT_TOKEN_TOPIC = "/event/FixtureReconnectToken__e";
final string FIXTURE_MULTI_EVENT_TOPIC = "/event/FixtureMultiEvent__e";
final string FIXTURE_PERMISSION_DENIED_TOPIC = "/event/FixturePermissionDenied__e";
final string FIXTURE_REPLAY_RECOVERY_TOPIC = "/event/FixtureReplayRecovery__e";
final string FIXTURE_STUCK_TOPIC = "/event/FixtureStuck__e";
final string FIXTURE_MULTI_CHUNK_TOPIC = "/event/FixtureMultiChunk__e";
final string FIXTURE_FLOW_CONTROL_TOPIC = "/event/FixtureFlowControl__e";
final string FIXTURE_AMBIGUOUS_CHUNK_TOPIC = "/event/FixtureAmbiguousChunk__e";
final string FIXTURE_ALWAYS_AMBIGUOUS_TOPIC = "/event/FixtureAlwaysAmbiguous__e";
// Must start with "/data/": that prefix is what routes an event through CDC
// normalization at all (see eventFromConsumerEvent in consumer_event.bal).
final string FIXTURE_MALFORMED_CDC_TOPIC = "/data/FixtureMalformedCdc__e";
final string FIXTURE_CDC_EVENTS_TOPIC = "/data/FixtureCdcEvents__e";
final string FIXTURE_INVALID_CDC_SCHEMA_TOPIC = "/data/FixtureInvalidCdcSchema__e";
final string FIXTURE_INVALID_CDC_SCHEMA_ID = "fixture-invalid-cdc-schema";
// Avro binary has no embedded type tags, so decoding against a "merely
// different" schema can silently succeed on whatever leading bytes happen to
// parse (this was tried and did not reliably fail). A fixed-size field far
// larger than any real payload reads a fixed number of raw bytes with no
// length prefix at all, so it reliably runs past the end of the buffer and
// throws regardless of the actual bytes' content.
final string FIXTURE_INVALID_CDC_SCHEMA = "{\"type\":\"record\",\"name\":\"FixtureWrongShape\",\"fields\":[" +
    "{\"name\":\"oversized\",\"type\":{\"type\":\"fixed\",\"name\":\"Oversized\",\"size\":10000}}]}";
final string FIXTURE_SCHEMA_ID = "fixture-schema";
final string FIXTURE_SCHEMA = "{\"type\":\"record\",\"name\":\"Fixture\",\"fields\":[{\"name\":\"Message__c\",\"type\":\"string\"}]}";
// An "evolved" writer schema: an event encoded against this one carries a
// field the original FIXTURE_SCHEMA never had.
final string FIXTURE_SCHEMA_V2_ID = "fixture-schema-v2";
final string FIXTURE_SCHEMA_V2 = "{\"type\":\"record\",\"name\":\"FixtureV2\",\"fields\":[" +
    "{\"name\":\"Message__c\",\"type\":\"string\"},{\"name\":\"Extra__c\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
final string FIXTURE_SCHEMA_EVOLUTION_TOPIC = "/event/FixtureSchemaEvolution__e";
final string FIXTURE_CDC_SCHEMA_ID = "fixture-cdc-schema";
final string FIXTURE_CDC_SCHEMA = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
    "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"ChangeEventHeader\",\"fields\":[" +
    "{\"name\":\"entityName\",\"type\":\"string\"},{\"name\":\"changeType\",\"type\":\"string\"}," +
    "{\"name\":\"changedFields\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}," +
    "{\"name\":\"nulledFields\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}," +
    "{\"name\":\"diffFields\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}}," +
    "{\"name\":\"Name\",\"type\":[\"null\",\"string\"]}]}";

final string[] FIXTURE_TOPICS = [
    FIXTURE_TOPIC, FIXTURE_ERROR_TOPIC, FIXTURE_RECONNECT_TOPIC, FIXTURE_RECONNECT_TOKEN_TOPIC, FIXTURE_MULTI_EVENT_TOPIC,
    FIXTURE_PERMISSION_DENIED_TOPIC, FIXTURE_REPLAY_RECOVERY_TOPIC, FIXTURE_STUCK_TOPIC,
    FIXTURE_MULTI_CHUNK_TOPIC, FIXTURE_AMBIGUOUS_CHUNK_TOPIC, FIXTURE_ALWAYS_AMBIGUOUS_TOPIC,
    FIXTURE_MALFORMED_CDC_TOPIC, FIXTURE_FLOW_CONTROL_TOPIC, FIXTURE_CDC_EVENTS_TOPIC,
    FIXTURE_INVALID_CDC_SCHEMA_TOPIC, FIXTURE_SCHEMA_EVOLUTION_TOPIC
];

isolated int fixtureReconnectTopicAttempts = 0;
isolated int fixtureReconnectTokenTopicAttempts = 0;
isolated int fixturePermissionDeniedAttempts = 0;
isolated int fixtureMultiChunkPublishCalls = 0;
isolated int fixtureMultiChunkPublishedEvents = 0;
isolated int fixtureAmbiguousChunkPublishCalls = 0;

isolated function nextFixtureReconnectAttempt() returns int {
    lock {
        fixtureReconnectTopicAttempts += 1;
        return fixtureReconnectTopicAttempts;
    }
}

isolated function nextFixtureReconnectTokenAttempt() returns int {
    lock {
        fixtureReconnectTokenTopicAttempts += 1;
        return fixtureReconnectTokenTopicAttempts;
    }
}

isolated function nextFixturePermissionDeniedAttempt() returns int {
    lock {
        fixturePermissionDeniedAttempts += 1;
        return fixturePermissionDeniedAttempts;
    }
}

isolated function fixturePermissionDeniedAttemptCount() returns int {
    lock {
        return fixturePermissionDeniedAttempts;
    }
}

isolated function recordFixtureMultiChunkPublish(int eventCount) returns int {
    lock {
        fixtureMultiChunkPublishedEvents += eventCount;
    }
    lock {
        fixtureMultiChunkPublishCalls += 1;
        return fixtureMultiChunkPublishCalls;
    }
}

isolated function fixtureMultiChunkPublishCallCount() returns int {
    lock {
        return fixtureMultiChunkPublishCalls;
    }
}

isolated function fixtureMultiChunkPublishedEventCount() returns int {
    lock {
        return fixtureMultiChunkPublishedEvents;
    }
}

isolated function nextFixtureAmbiguousChunkPublishCall() returns int {
    lock {
        fixtureAmbiguousChunkPublishCalls += 1;
        return fixtureAmbiguousChunkPublishCalls;
    }
}

isolated int fixtureAlwaysAmbiguousPublishAttempts = 0;

isolated function recordFixtureAlwaysAmbiguousPublishAttempt() returns int {
    lock {
        fixtureAlwaysAmbiguousPublishAttempts += 1;
        return fixtureAlwaysAmbiguousPublishAttempts;
    }
}

isolated function fixtureAlwaysAmbiguousPublishAttemptCount() returns int {
    lock {
        return fixtureAlwaysAmbiguousPublishAttempts;
    }
}

function drainFixtureRequests(stream<wire:FetchRequest, grpc:Error?> requests) {
    while true {
        record {|wire:FetchRequest value;|}|grpc:Error? next = requests.next();
        if next is () || next is grpc:Error {
            return;
        }
    }
}

// Yields one response, then stays idle for a bounded period instead of
// immediately signaling stream exhaustion. A stream that naturally exhausts
// right after its one response causes the client to conclude the response was
// fully received, after which it refuses to send further requests on that
// stream ("Inbound response message already received") -- fatal for a
// scenario needing per-event replenishment FetchRequests after the batch.
// Staying open for a bit (matching a real, still-live Subscribe stream idling
// with nothing more to deliver) keeps the client willing to send. The idle
// period is deliberately bounded (not indefinite): every test using this ties
// up one of the fixture server's own request-handling slots for that long,
// and with many fixture-based tests in this suite, indefinite blocking here
// previously exhausted the shared local listener's capacity and caused
// unrelated later tests to fail to connect at all.
final decimal FIXTURE_IDLE_SECONDS = 2;

isolated class FixtureOneResponseThenIdleStream {
    private final wire:FetchResponse & readonly response;
    private boolean served = false;

    isolated function init(wire:FetchResponse response) {
        self.response = response.cloneReadOnly();
    }

    public isolated function next() returns record {|wire:FetchResponse value;|}|error? {
        boolean shouldServe;
        lock {
            shouldServe = !self.served;
            self.served = true;
        }
        if shouldServe {
            return {value: self.response};
        }
        runtime:sleep(FIXTURE_IDLE_SECONDS);
        return ();
    }
}

// For the one test that specifically proves gracefulStop's wait is bounded
// rather than infinite: this must genuinely still be blocked well past the
// connector's shutdown timeout, unlike FixtureOneResponseThenIdleStream's
// short, resource-friendly idle period.
isolated class FixtureStuckAfterOneResponseStream {
    private final wire:FetchResponse & readonly response;
    private boolean served = false;

    isolated function init(wire:FetchResponse response) {
        self.response = response.cloneReadOnly();
    }

    public isolated function next() returns record {|wire:FetchResponse value;|}|error? {
        boolean shouldServe;
        lock {
            shouldServe = !self.served;
            self.served = true;
        }
        if shouldServe {
            return {value: self.response};
        }
        runtime:sleep(60);
        return ();
    }
}

final int FIXTURE_FLOW_CONTROL_BUFFER_SIZE = 2;
final int FIXTURE_FLOW_CONTROL_TOTAL_EVENTS = 4;
isolated int fixtureFlowControlNonPositiveCreditRequests = 0;

isolated function recordFixtureFlowControlCredit(int numRequested) {
    lock {
        if numRequested <= 0 {
            fixtureFlowControlNonPositiveCreditRequests += 1;
        }
    }
}

isolated function fixtureFlowControlNonPositiveCreditRequestCount() returns int {
    lock {
        return fixtureFlowControlNonPositiveCreditRequests;
    }
}

// Unlike drainFixtureRequests, records every FetchRequest's credit instead of
// silently discarding it, so a test can prove the connector never sends a
// zero (or negative) replacement credit request over the real wire. The two
// canned responses this topic serves (see FixtureFlowControlResponses below)
// are sized to match the client's own credit accounting regardless of when
// this validation actually observes each request arriving: the client
// increments its local outstanding-credit count synchronously, before it
// even loops back to receive the next response, so response pacing here
// doesn't need to (and cannot usefully) wait for a specific request first.
function validateFixtureFlowControlRequests(stream<wire:FetchRequest, grpc:Error?> requests) {
    while true {
        record {|wire:FetchRequest value;|}|grpc:Error? next = requests.next();
        if next is () || next is grpc:Error {
            return;
        }
        recordFixtureFlowControlCredit(next.value.num_requested);
    }
}

// A bufferSize-of-2 subscription to a 4-event topic requests 2 initially,
// then (after both of those checkpoint) 2 more one-credit-at-a-time
// replenishment requests before the next response is needed -- so two
// 2-event responses is exactly what a correctly credit-paced client needs.
isolated function fixtureFlowControlResponses() returns wire:FetchResponse[]|error {
    wire:ConsumerEvent[] batch1 = [];
    wire:ConsumerEvent[] batch2 = [];
    foreach int i in 1 ... FIXTURE_FLOW_CONTROL_TOTAL_EVENTS {
        byte[] eventPayload = check encodePayload(FIXTURE_SCHEMA, {"Message__c": "flow-" + i.toString()});
        wire:ConsumerEvent event = {
            event: {id: "flow-event-" + i.toString(), schema_id: FIXTURE_SCHEMA_ID, payload: eventPayload},
            replay_id: [<byte>i]
        };
        if i <= FIXTURE_FLOW_CONTROL_BUFFER_SIZE {
            batch1.push(event);
        } else {
            batch2.push(event);
        }
    }
    return [{events: batch1}, {events: batch2}];
}

// Serves a fixed sequence of canned responses (unlike
// FixtureOneResponseThenIdleStream's single response), then idles.
isolated class FixtureMultiResponseThenIdleStream {
    private final wire:FetchResponse[] & readonly responses;
    private int served = 0;

    isolated function init(wire:FetchResponse[] responses) {
        self.responses = responses.cloneReadOnly();
    }

    public isolated function next() returns record {|wire:FetchResponse value;|}|error? {
        int index;
        lock {
            index = self.served;
            self.served += 1;
        }
        if index < self.responses.length() {
            return {value: self.responses[index]};
        }
        runtime:sleep(FIXTURE_IDLE_SECONDS);
        return ();
    }
}

listener grpc:Listener fixtureListener = new (FIXTURE_PORT, {
    host: "localhost",
    secureSocket: {
        key: {
            certFile: "tests/resources/local-grpc.crt",
            keyFile: "tests/resources/local-grpc.key"
        }
    }
});

@grpc:Descriptor {value: wire:PUBSUB_API_DESC}
service "PubSub" on fixtureListener {
    remote function GetSchema(wire:SchemaRequest request) returns wire:SchemaInfo|error {
        if request.schema_id == FIXTURE_CDC_SCHEMA_ID {
            return {schema_json: FIXTURE_CDC_SCHEMA, schema_id: FIXTURE_CDC_SCHEMA_ID};
        }
        if request.schema_id == FIXTURE_INVALID_CDC_SCHEMA_ID {
            // Deliberately wrong: the event bytes for this schema ID are
            // encoded with FIXTURE_CDC_SCHEMA, but GetSchema serves this
            // unrelated schema for it -- simulating Salesforce returning a
            // stale or incorrect writer schema, distinct from a malformed
            // CDC bitmap in an otherwise-correct decode.
            return {schema_json: FIXTURE_INVALID_CDC_SCHEMA, schema_id: FIXTURE_INVALID_CDC_SCHEMA_ID};
        }
        if request.schema_id == FIXTURE_SCHEMA_V2_ID {
            return {schema_json: FIXTURE_SCHEMA_V2, schema_id: FIXTURE_SCHEMA_V2_ID};
        }
        if request.schema_id != FIXTURE_SCHEMA_ID {
            return error("unexpected schema request");
        }
        return {schema_json: FIXTURE_SCHEMA, schema_id: FIXTURE_SCHEMA_ID};
    }

    remote function GetTopic(wire:TopicRequest request) returns wire:TopicInfo|error {
        if FIXTURE_TOPICS.indexOf(request.topic_name) is () {
            return error("unexpected topic request");
        }
        string schemaId = FIXTURE_SCHEMA_ID;
        if request.topic_name == FIXTURE_MALFORMED_CDC_TOPIC || request.topic_name == FIXTURE_CDC_EVENTS_TOPIC {
            schemaId = FIXTURE_CDC_SCHEMA_ID;
        } else if request.topic_name == FIXTURE_INVALID_CDC_SCHEMA_TOPIC {
            schemaId = FIXTURE_INVALID_CDC_SCHEMA_ID;
        }
        return {topic_name: request.topic_name, can_publish: true, can_subscribe: true, schema_id: schemaId};
    }

    remote function Publish(wire:PublishRequest request) returns wire:PublishResponse|error {
        if request.topic_name == FIXTURE_AMBIGUOUS_CHUNK_TOPIC {
            if nextFixtureAmbiguousChunkPublishCall() == 2 {
                return error("simulated ambiguous publish transport failure");
            }
        }
        if request.topic_name == FIXTURE_ALWAYS_AMBIGUOUS_TOPIC {
            // Simulates a timeout after Salesforce may have already accepted
            // the request: every attempt for this topic ends without a
            // definitive response, so it must never be retried automatically.
            _ = recordFixtureAlwaysAmbiguousPublishAttempt();
            return error("simulated timeout after possible server acceptance");
        }
        if request.topic_name == FIXTURE_MULTI_CHUNK_TOPIC {
            _ = recordFixtureMultiChunkPublish(request.events.length());
        }
        wire:PublishResult[] results = [];
        foreach wire:ProducerEvent event in request.events {
            results.push({replay_id: [1, 2, 3], correlation_key: event.id});
        }
        return {results, schema_id: FIXTURE_SCHEMA_ID};
    }

    remote function Subscribe(stream<wire:FetchRequest, grpc:Error?> requests)
            returns stream<wire:FetchResponse, error?>|error {
        record {|wire:FetchRequest value;|}? first = check requests.next();
        if first is () {
            return error("no fetch request received");
        }
        if first.value.topic_name == FIXTURE_FLOW_CONTROL_TOPIC {
            recordFixtureFlowControlCredit(first.value.num_requested);
            future<()> _ = start validateFixtureFlowControlRequests(requests);
            return new stream<wire:FetchResponse, error?>(new FixtureMultiResponseThenIdleStream(check fixtureFlowControlResponses()));
        }
        // The generated bidi dispatch stops accepting inbound messages soon
        // after this handler body returns its response value, so a client's
        // later per-event replenishment FetchRequests would otherwise start
        // failing mid-batch. Keep draining requests in the background so the
        // server keeps accepting them for as long as the connection is alive.
        future<()> _ = start drainFixtureRequests(requests);
        if first.value.topic_name == FIXTURE_ERROR_TOPIC {
            return error grpc:UnavailableError("simulated subscribe stream failure");
        }
        if first.value.topic_name == FIXTURE_RECONNECT_TOPIC {
            if nextFixtureReconnectAttempt() == 1 {
                // A normal, non-transport-level closure: reconnectable, but
                // must not force recreating the shared channel.
                return error("Subscribe stream closed");
            }
            byte[] reconnectPayload = check encodePayload(FIXTURE_SCHEMA, {"Message__c": "reconnected"});
            wire:FetchResponse reconnectResponse = {
                events: [{event: {id: "reconnected-event", schema_id: FIXTURE_SCHEMA_ID, payload: reconnectPayload}, replay_id: [4, 5, 6]}]
            };
            return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream(reconnectResponse));
        }
        if first.value.topic_name == FIXTURE_RECONNECT_TOKEN_TOPIC {
            if nextFixtureReconnectTokenAttempt() == 1 {
                return error("Subscribe stream closed");
            }
            byte[] reconnectPayload = check encodePayload(FIXTURE_SCHEMA, {"Message__c": "reconnected"});
            wire:FetchResponse reconnectResponse = {
                events: [{event: {id: "reconnected-event", schema_id: FIXTURE_SCHEMA_ID, payload: reconnectPayload}, replay_id: [4, 5, 6]}]
            };
            return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream(reconnectResponse));
        }
        if first.value.topic_name == FIXTURE_PERMISSION_DENIED_TOPIC {
            _ = nextFixturePermissionDeniedAttempt();
            return error grpc:PermissionDeniedError("user lacks Pub/Sub access");
        }
        if first.value.topic_name == FIXTURE_REPLAY_RECOVERY_TOPIC {
            if first.value.replay_preset == wire:CUSTOM {
                // Simulates Salesforce rejecting a stale/expired stored cursor.
                return error grpc:FailedPreconditionError("replay id validation failed");
            }
            byte[] recoveredPayload = check encodePayload(FIXTURE_SCHEMA, {"Message__c": "recovered"});
            wire:FetchResponse recoveredResponse = {
                events: [{event: {id: "recovered-event", schema_id: FIXTURE_SCHEMA_ID, payload: recoveredPayload}, replay_id: [11, 12, 13]}]
            };
            return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream(recoveredResponse));
        }
        if first.value.topic_name == FIXTURE_STUCK_TOPIC {
            byte[] stuckPayload = check encodePayload(FIXTURE_SCHEMA, {"Message__c": "stuck"});
            wire:FetchResponse stuckResponse = {
                events: [{event: {id: "stuck-event", schema_id: FIXTURE_SCHEMA_ID, payload: stuckPayload}, replay_id: [1, 2, 3]}]
            };
            return new stream<wire:FetchResponse, error?>(new FixtureStuckAfterOneResponseStream(stuckResponse));
        }
        if first.value.topic_name == FIXTURE_MALFORMED_CDC_TOPIC {
            byte[] cdcPayload = check encodePayload(FIXTURE_CDC_SCHEMA, {
                "ChangeEventHeader": {
                    "entityName": "Account",
                    "changeType": "UPDATE",
                    "changedFields": ["not-a-bitmap"],
                    "nulledFields": [],
                    "diffFields": []
                },
                "Name": "Acme"
            });
            wire:FetchResponse cdcResponse = {
                events: [{event: {id: "malformed-cdc-event", schema_id: FIXTURE_CDC_SCHEMA_ID, payload: cdcPayload}, replay_id: [1]}]
            };
            return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream(cdcResponse));
        }
        if first.value.topic_name == FIXTURE_CDC_EVENTS_TOPIC {
            // "0x02" sets bit 1 in FIXTURE_CDC_SCHEMA's field order
            // (ChangeEventHeader=0, Name=1), i.e. Name changed.
            byte[] createPayload = check encodePayload(FIXTURE_CDC_SCHEMA, {
                "ChangeEventHeader": {
                    "entityName": "Account", "changeType": "CREATE",
                    "changedFields": ["0x02"], "nulledFields": [], "diffFields": []
                },
                "Name": "Acme Inc"
            });
            byte[] updatePayload = check encodePayload(FIXTURE_CDC_SCHEMA, {
                "ChangeEventHeader": {
                    "entityName": "Account", "changeType": "UPDATE",
                    "changedFields": ["0x02"], "nulledFields": [], "diffFields": []
                },
                "Name": "Acme International"
            });
            byte[] deletePayload = check encodePayload(FIXTURE_CDC_SCHEMA, {
                "ChangeEventHeader": {
                    "entityName": "Account", "changeType": "DELETE",
                    "changedFields": [], "nulledFields": [], "diffFields": []
                },
                "Name": ()
            });
            wire:FetchResponse cdcEventsResponse = {
                events: [
                    {event: {id: "cdc-create", schema_id: FIXTURE_CDC_SCHEMA_ID, payload: createPayload}, replay_id: [1]},
                    {event: {id: "cdc-update", schema_id: FIXTURE_CDC_SCHEMA_ID, payload: updatePayload}, replay_id: [2]},
                    {event: {id: "cdc-delete", schema_id: FIXTURE_CDC_SCHEMA_ID, payload: deletePayload}, replay_id: [3]}
                ]
            };
            return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream(cdcEventsResponse));
        }
        if first.value.topic_name == FIXTURE_INVALID_CDC_SCHEMA_TOPIC {
            byte[] mismatchedPayload = check encodePayload(FIXTURE_CDC_SCHEMA, {
                "ChangeEventHeader": {
                    "entityName": "Account", "changeType": "UPDATE",
                    "changedFields": ["0x02"], "nulledFields": [], "diffFields": []
                },
                "Name": "Acme"
            });
            wire:FetchResponse invalidSchemaResponse = {
                events: [{
                    event: {id: "invalid-schema-event", schema_id: FIXTURE_INVALID_CDC_SCHEMA_ID, payload: mismatchedPayload},
                    replay_id: [1]
                }]
            };
            return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream(invalidSchemaResponse));
        }
        if first.value.topic_name == FIXTURE_SCHEMA_EVOLUTION_TOPIC {
            // Two events in the same batch, each encoded against and tagged
            // with its own distinct schema ID -- proving the connector
            // resolves and decodes every event with the schema recorded on
            // that event specifically, not whichever schema was resolved
            // first for the batch/topic.
            byte[] oldPayload = check encodePayload(FIXTURE_SCHEMA, {"Message__c": "old-shape"});
            byte[] newPayload = check encodePayload(FIXTURE_SCHEMA_V2, {"Message__c": "new-shape", "Extra__c": "added-field"});
            wire:FetchResponse evolutionResponse = {
                events: [
                    {event: {id: "evolution-old", schema_id: FIXTURE_SCHEMA_ID, payload: oldPayload}, replay_id: [1]},
                    {event: {id: "evolution-new", schema_id: FIXTURE_SCHEMA_V2_ID, payload: newPayload}, replay_id: [2]}
                ]
            };
            return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream(evolutionResponse));
        }
        if first.value.topic_name == FIXTURE_MULTI_EVENT_TOPIC {
            wire:ConsumerEvent[] events = [];
            foreach int i in 1 ... 10 {
                byte[] eventPayload = check encodePayload(FIXTURE_SCHEMA, {"Message__c": "event-" + i.toString()});
                events.push({event: {id: "multi-event-" + i.toString(), schema_id: FIXTURE_SCHEMA_ID, payload: eventPayload}, replay_id: [<byte>i]});
            }
            return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream({events}));
        }
        byte[] payload = check encodePayload(FIXTURE_SCHEMA, {"Message__c": "streamed"});
        wire:FetchResponse response = {
            events: [{event: {id: "streamed-event", schema_id: FIXTURE_SCHEMA_ID, payload}, replay_id: [7, 8, 9]}]
        };
        return new stream<wire:FetchResponse, error?>(new FixtureOneResponseThenIdleStream(response));
    }

    remote function PublishStream(stream<wire:PublishRequest, grpc:Error?> requests)
            returns stream<wire:PublishResponse, error?>|error {
        return error("not implemented by local fixture");
    }

    remote function ManagedSubscribe(stream<wire:ManagedFetchRequest, grpc:Error?> requests)
            returns stream<wire:ManagedFetchResponse, error?>|error {
        return error("not implemented by local fixture");
    }
}

// This fails if the public Publisher does not reach the real generated gRPC
// transport with an explicit local TLS trust root and return its wire result.
@test:Config {}
function testPublisherUsesLocalTlsGrpcFixture() returns error? {
    Publisher publisher = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        topic: FIXTURE_TOPIC
    });

    PublishResult[] results = check publisher->publish([{payload: {"Message__c": "fixture"}, id: "event-1"}]);
    test:assertEquals(results.length(), 1);
    test:assertEquals(results[0].id, "event-1");
    test:assertEquals(results[0].replayId, [1, 2, 3]);
}

// This fails if a connection with no configured trust root silently accepts
// the fixture's self-signed certificate instead of failing the TLS handshake,
// or if the resulting error leaks the configured bearer token.
@test:Config {}
function testPublisherFailsClosedWithoutTrustingTheFixtureCertificate() returns error? {
    Publisher publisher = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token-should-not-leak"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString()
        },
        topic: FIXTURE_TOPIC
    });

    wire:TopicInfo|error result = publisher->getTopic();
    test:assertTrue(result is error, "an untrusted self-signed certificate must fail the TLS handshake");
    if result is error {
        test:assertFalse(result.message().includes("fixture-token-should-not-leak"),
            "a TLS failure must never surface the configured access token");
    }
}

// This fails if a batch larger than the configured request-size target is
// sent as one oversized Publish RPC instead of being transparently split, or
// if the split results lose their original order/correlation.
@test:Config {}
function testPublisherSplitsLargeBatchAcrossMultiplePublishRpcs() returns error? {
    PublishEvent[] events = [];
    foreach int i in 1 ... 5 {
        events.push({payload: {"Message__c": "event-" + i.toString()}, id: "event-" + i.toString()});
    }
    // Computed (not guessed) from the real encoded size, so this stays
    // correct however the wire encoding or protobuf overhead changes: fits
    // exactly two of these same-size events, forcing a three-way split.
    wire:ProducerEvent[] previewEvents = check producerEventsFor(FIXTURE_SCHEMA, FIXTURE_SCHEMA_ID, events);
    int topicBase = protobufBytesFieldSize(FIXTURE_MULTI_CHUNK_TOPIC.toBytes().length());
    int targetForTwoPerChunk = topicBase + (2 * producerEventRequestContribution(previewEvents[0]));

    Publisher publisher = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        topic: FIXTURE_MULTI_CHUNK_TOPIC,
        targetRequestSizeBytes: targetForTwoPerChunk
    });

    PublishResult[] results = check publisher->publish(events);
    test:assertEquals(results.length(), 5);
    foreach int i in 0 ... 4 {
        test:assertEquals(results[i].id, "event-" + (i + 1).toString());
        test:assertEquals(results[i].replayId, [1, 2, 3]);
    }
    // 5 events split at 2 per request must take 3 Publish RPCs (2, 2, 1).
    test:assertEquals(fixtureMultiChunkPublishCallCount(), 3);
    test:assertEquals(fixtureMultiChunkPublishedEventCount(), 5);
}

// This fails if one chunk's ambiguous Publish outcome causes another chunk's
// already-definitive results to be discarded instead of carried on the error.
@test:Config {}
function testPublisherReportsAmbiguousChunkWithoutDiscardingSiblingResults() returns error? {
    PublishEvent[] events = [
        {payload: {"Message__c": "first"}, id: "event-1"},
        {payload: {"Message__c": "second"}, id: "event-2"}
    ];
    // Computed so exactly one event fits per chunk regardless of the two
    // payloads' exact encoded sizes: the target is set to the larger of the
    // two contributions, so a second event of either size always overflows it.
    wire:ProducerEvent[] previewEvents = check producerEventsFor(FIXTURE_SCHEMA, FIXTURE_SCHEMA_ID, events);
    int topicBase = protobufBytesFieldSize(FIXTURE_AMBIGUOUS_CHUNK_TOPIC.toBytes().length());
    int firstContribution = producerEventRequestContribution(previewEvents[0]);
    int secondContribution = producerEventRequestContribution(previewEvents[1]);
    int targetForOnePerChunk = topicBase + (firstContribution > secondContribution ? firstContribution : secondContribution);

    Publisher publisher = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        topic: FIXTURE_AMBIGUOUS_CHUNK_TOPIC,
        targetRequestSizeBytes: targetForOnePerChunk
    });

    PublishResult[]|error result = publisher->publish(events);
    test:assertTrue(result is error<AmbiguousPublishDetail>);
    if result is error<AmbiguousPublishDetail> {
        AmbiguousPublishDetail detail = result.detail();
        test:assertEquals(detail.topic, FIXTURE_AMBIGUOUS_CHUNK_TOPIC);
        test:assertEquals(detail.eventIds, ["event-2"]);
        test:assertEquals(detail.definitiveResults.length(), 1);
        test:assertEquals(detail.definitiveResults[0].id, "event-1");
        test:assertEquals(detail.definitiveResults[0].replayId, [1, 2, 3]);
    }
}

// This fails if an ambiguous Publish outcome (a timeout after Salesforce may
// have already accepted the request) is retried automatically. D20 requires
// exactly one attempt per chunk; a retry risks a duplicate publish Salesforce
// itself may already have accepted.
@test:Config {}
function testPublisherNeverRetriesAnAmbiguousChunkAutomatically() returns error? {
    Publisher publisher = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        topic: FIXTURE_ALWAYS_AMBIGUOUS_TOPIC
    });

    PublishResult[]|error result = publisher->publish([{payload: {"Message__c": "only"}, id: "event-1"}]);
    test:assertTrue(result is error<AmbiguousPublishDetail>);
    if result is error<AmbiguousPublishDetail> {
        test:assertEquals(result.detail().eventIds, ["event-1"]);
    }
    test:assertEquals(fixtureAlwaysAmbiguousPublishAttemptCount(), 1);
}

// This fails if Listener start cannot establish an authenticated TLS stream,
// decode a writer-schema event, and checkpoint its exact replay identifier.
@test:Config {}
function testListenerUsesLocalTlsGrpcFixture() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            test:assertEquals(event.payload["Message__c"], "streamed");
        }
    };
    check endpoint.attach(handler, FIXTURE_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.2);
    // immediateStop() is used because this test only needs the already-saved
    // checkpoint, not a bounded graceful wait; see
    // testListenerGracefulStopBoundsWaitOnAnIdleStream (FIXTURE_STUCK_TOPIC)
    // for that, which needs a stream that stays genuinely blocked far longer
    // than FIXTURE_TOPIC's short, resource-friendly idle period.
    check endpoint.immediateStop();
    byte[]? replayId = check replayStore.load({
        tenantId: "00DFixture000001",
        topic: FIXTURE_TOPIC,
        subscriptionName: "default"
    });
    test:assertEquals(replayId, [7, 8, 9]);
}

// This fails if gracefulStop does not bound its wait for a topic worker that
// is genuinely still blocked (here, in a pending receive on a stream that
// stays open with no further events) to roughly the connector-owned
// graceful-shutdown timeout, instead of hanging indefinitely.
@test:Config {}
function testListenerGracefulStopBoundsWaitOnAnIdleStream() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    check endpoint.attach(handler, FIXTURE_STUCK_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.2);
    time:Utc before = time:utcNow();
    check endpoint.gracefulStop();
    decimal elapsed = time:utcDiffSeconds(time:utcNow(), before);
    test:assertTrue(elapsed < 6.5d, "gracefulStop should not hang past its timeout, took " + elapsed.toString() + "s");
    test:assertTrue(elapsed > 3.0d, "expected gracefulStop to actually wait out the timeout, took " + elapsed.toString() + "s");
}

// This fails if a small bufferSize ever lets a replacement FetchRequest carry
// zero (or negative) credit, or if draining more events than one batch's
// initial credit loses or reorders any of them across the several
// replenishment round trips a buffer of 2 forces for 4 total events.
@test:Config {}
function testListenerBoundsWireCreditAcrossMultipleReplenishmentRoundTrips() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {bufferSize: FIXTURE_FLOW_CONTROL_BUFFER_SIZE, reconnectRetry: {maxRetries: 0}}
    });
    FixtureFailingAtService handler = new ("never-fails");
    check endpoint.attach(handler, FIXTURE_FLOW_CONTROL_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.5);
    check endpoint.immediateStop();

    test:assertEquals(handler.messages(), ["flow-1", "flow-2", "flow-3", "flow-4"]);
    test:assertEquals(fixtureFlowControlNonPositiveCreditRequestCount(), 0);
}

// This fails if a reconnectable Subscribe failure does not actually reopen a
// fresh stream and resume delivery: it asserts the checkpoint reflects the
// second connection's distinct event, not just a repeat of the first.
@test:Config {}
function testListenerReconnectsAndResumesDeliveryAfterStreamClosure() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {
            handlerRetry: {maxRetries: 0},
            reconnectRetry: {maxRetries: 1, initialDelay: 0.05, maxDelay: 0.05}
        }
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    check endpoint.attach(handler, FIXTURE_RECONNECT_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.5);
    check endpoint.immediateStop();
    byte[]? replayId = check replayStore.load({
        tenantId: "00DFixture000001",
        topic: FIXTURE_RECONNECT_TOPIC,
        subscriptionName: "default"
    });
    test:assertEquals(replayId, [4, 5, 6]);
}

// This fails if reconnecting a closed stream skips resolving a token through
// the configured OAuth grant (the reconnect path is exercised only via
// openTopicSubscription being re-invoked; nothing here special-cases it), or
// if it wastefully fetches a brand-new token on every reconnect attempt
// instead of reusing one that is still valid.
@test:Config {}
function testListenerReusesValidRefreshTokenAcrossReconnect() returns error? {
    int requestsBefore;
    lock {
        requestsBefore = tokenFixtureRequests;
    }
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:OAuth2RefreshTokenGrantConfig>{
                refreshUrl: OAUTH_FIXTURE_URL,
                refreshToken: "seed-refresh-token",
                clientId: "reconnect-refresh-client",
                clientSecret: "refresh-secret"
            },
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {
            handlerRetry: {maxRetries: 0},
            reconnectRetry: {maxRetries: 1, initialDelay: 0.05, maxDelay: 0.05}
        }
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    check endpoint.attach(handler, FIXTURE_RECONNECT_TOKEN_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.5);
    check endpoint.immediateStop();

    byte[]? replayId = check replayStore.load({
        tenantId: "00DFixture000001",
        topic: FIXTURE_RECONNECT_TOKEN_TOPIC,
        subscriptionName: "default"
    });
    test:assertEquals(replayId, [4, 5, 6], "expected the listener to reconnect and resume delivery");
    lock {
        test:assertEquals(tokenFixtureRequests, requestsBefore + 1,
            "the initial stream open and the reconnect should share one still-valid token, not one fetch each");
    }
}

isolated service class FixtureCountingService {
    *Service;
    private int count = 0;

    remote function onEvent(Event event) returns error? {
        lock {
            self.count += 1;
        }
    }

    isolated function deliveryCount() returns int {
        lock {
            return self.count;
        }
    }
}

// Fails permanently for one designated event (matched by payload message) so
// mid-batch handler-failure behavior can be observed, and records every
// message it was actually invoked with, in order.
isolated service class FixtureFailingAtService {
    *Service;
    private final string failingMessage;
    private string[] deliveredMessages = [];

    isolated function init(string failingMessage) {
        self.failingMessage = failingMessage;
    }

    remote function onEvent(Event event) returns error? {
        anydata message = event.payload["Message__c"];
        lock {
            self.deliveredMessages.push(<string>message.clone());
        }
        if message == self.failingMessage {
            return error("simulated permanent handler failure for " + self.failingMessage);
        }
    }

    isolated function messages() returns string[] {
        lock {
            return self.deliveredMessages.clone();
        }
    }
}

// A ReplayStore wrapper whose very next `save` fails exactly once, then
// delegates normally. Used to prove a checkpoint-save failure halts
// progression without advancing the durable cursor, and that the event is
// redelivered by a later Listener sharing the same store.
isolated class FailOnceReplayStore {
    *ReplayStore;
    private final InMemoryReplayStore delegate = new;
    private boolean shouldFailNextSave;

    isolated function init(boolean shouldFailNextSave) {
        self.shouldFailNextSave = shouldFailNextSave;
    }

    public isolated function load(ReplayKey key) returns byte[]|error? {
        return self.delegate.load(key);
    }

    public isolated function save(ReplayKey key, byte[] replayId) returns error? {
        boolean shouldFail;
        lock {
            shouldFail = self.shouldFailNextSave;
            self.shouldFailNextSave = false;
        }
        if shouldFail {
            return error("simulated checkpoint save failure");
        }
        return self.delegate.save(key, replayId);
    }
}

// Records "<changeType>:<Name or <absent>>" for every delivered CDC event, in
// order, so a test can confirm each change type normalizes distinctly (a
// CREATE/UPDATE's changed Name value present in changedData, a DELETE's
// absent) without needing to capture full payload structures per test.
isolated service class FixtureCdcRecorderService {
    *Service;
    private string[] recordedDeliveries = [];

    remote function onEvent(Event event) returns error? {
        map<anydata> metadata = check event.payload["metadata"].ensureType();
        map<anydata> changedData = check event.payload["changedData"].ensureType();
        string changeType = <string>metadata["changeType"];
        anydata? name = changedData["Name"];
        string entry = changeType + ":" + (name is string ? name : "<absent>");
        lock {
            self.recordedDeliveries.push(entry);
        }
    }

    isolated function deliveries() returns string[] {
        lock {
            return self.recordedDeliveries.clone();
        }
    }
}

// Records "<Message__c>:<Extra__c or <absent>>" for every delivered event, so
// a schema-evolution test can confirm each event decoded with its own
// envelope schema rather than, say, the batch's first-resolved schema.
isolated service class FixtureSchemaEvolutionRecorderService {
    *Service;
    private string[] recordedDeliveries = [];

    remote function onEvent(Event event) returns error? {
        string message = <string>event.payload["Message__c"];
        anydata? extra = event.payload["Extra__c"];
        string entry = message + ":" + (extra is string ? extra : "<absent>");
        lock {
            self.recordedDeliveries.push(entry);
        }
    }

    isolated function deliveries() returns string[] {
        lock {
            return self.recordedDeliveries.clone();
        }
    }
}

isolated service class FixtureErrorAwareService {
    *Service;
    private boolean notified = false;
    private ListenerError? receivedError = ();

    remote function onEvent(Event event) returns error? {
        return ();
    }

    remote function onError(ListenerError err) returns error? {
        lock {
            self.notified = true;
            self.receivedError = err.cloneReadOnly();
        }
    }

    isolated function wasNotified() returns boolean {
        lock {
            return self.notified;
        }
    }
}

// This fails if a real transport-level Subscribe failure does not stop the
// Listener, record a terminal ListenerError, and invoke an attached service's
// optional onError callback.
@test:Config {}
function testListenerNotifiesOnErrorAndRecordsTerminalFailureOnStreamError() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    FixtureErrorAwareService handler = new;
    check endpoint.attach(handler, FIXTURE_ERROR_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    ListenerError? lastError = endpoint.getLastError();
    test:assertTrue(lastError is ListenerError);
    if lastError is ListenerError {
        test:assertEquals(lastError.grpcStatus, "UNAVAILABLE");
    }
    test:assertTrue(handler.wasNotified());
    check endpoint.immediateStop();
}

// This fails if a handler failure on event 7 of a 10-event batch does not
// leave the checkpoint at event 6 and permit events 8-10 to reach onEvent:
// the "hold later events, checkpoint stays behind the unresolved event"
// contract from D4/D2.
@test:Config {}
function testListenerHandlerFailureMidBatchHoldsLaterEventsAndChecksAtLastGood() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    FixtureFailingAtService handler = new ("event-7");
    check endpoint.attach(handler, FIXTURE_MULTI_EVENT_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    test:assertTrue(endpoint.getLastError() is ListenerError);
    test:assertEquals(handler.messages(), ["event-1", "event-2", "event-3", "event-4", "event-5", "event-6", "event-7"]);
    byte[]? replayId = check replayStore.load({
        tenantId: "00DFixture000001",
        topic: FIXTURE_MULTI_EVENT_TOPIC,
        subscriptionName: "default"
    });
    test:assertEquals(replayId, [6]);
    // The terminal failure already closed the stream; this only confirms
    // stopping an already-self-terminated Listener is still safe.
    check endpoint.immediateStop();
}

// This fails if a checkpoint-save failure either advances the durable cursor
// past the unresolved event or is silently dropped instead of stopping the
// Listener: D5 requires progression to stop, and a later Listener sharing the
// same store must redeliver the never-checkpointed event rather than skip it.
@test:Config {}
function testListenerRedeliversEventAfterCheckpointSaveFailure() returns error? {
    FailOnceReplayStore replayStore = new (true);
    ConnectionConfig connection = {
        auth: <http:BearerTokenConfig>{token: "fixture-token"},
        instanceUrl: "https://fixture.my.salesforce.com",
        tenantId: "00DFixture000001",
        endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
        grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
    };
    ReplayKey checkpointKey = {tenantId: "00DFixture000001", topic: FIXTURE_TOPIC, subscriptionName: "default"};

    Listener firstEndpoint = check new ({
        connection,
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    FixtureCountingService firstHandler = new;
    check firstEndpoint.attach(firstHandler, FIXTURE_TOPIC);
    check firstEndpoint.'start();
    runtime:sleep(0.3);
    test:assertTrue(firstEndpoint.getLastError() is ListenerError);
    test:assertEquals(firstHandler.deliveryCount(), 1);
    byte[]? checkpointAfterFailure = check replayStore.load(checkpointKey);
    test:assertTrue(checkpointAfterFailure is (), "a failed checkpoint save must not leave a durable cursor");
    check firstEndpoint.immediateStop();

    // A fresh Listener over the same store and topic simulates a restart.
    // Since nothing was ever durably checkpointed, the same event must be
    // redelivered rather than skipped.
    Listener secondEndpoint = check new ({
        connection,
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    FixtureCountingService secondHandler = new;
    check secondEndpoint.attach(secondHandler, FIXTURE_TOPIC);
    check secondEndpoint.'start();
    runtime:sleep(0.3);
    check secondEndpoint.immediateStop();
    test:assertEquals(secondHandler.deliveryCount(), 1);
    byte[]? checkpointAfterRestart = check replayStore.load(checkpointKey);
    test:assertEquals(checkpointAfterRestart, [7, 8, 9]);
}

// This fails if a non-reconnectable Subscribe failure (an ordinary permission
// error) is retried instead of stopping the Listener on the first attempt.
@test:Config {}
function testListenerTreatsPermissionDeniedAsTerminalWithoutRetry() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {
            handlerRetry: {maxRetries: 0},
            reconnectRetry: {maxRetries: 3, initialDelay: 0.05, maxDelay: 0.05}
        }
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    check endpoint.attach(handler, FIXTURE_PERMISSION_DENIED_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    ListenerError? lastError = endpoint.getLastError();
    test:assertTrue(lastError is ListenerError);
    if lastError is ListenerError {
        test:assertEquals(lastError.grpcStatus, "PERMISSION_DENIED");
    }
    test:assertEquals(fixturePermissionDeniedAttemptCount(), 1);
    check endpoint.immediateStop();
}

// This fails if a malformed CDC event (an undecodable bitmap here) either
// reaches the application handler or is silently skipped instead of
// following Task 10's terminal-listener-stop path: decode/normalization
// failures happen before delivery, so they are not a handler failure and are
// never handler-retried, but the Listener must still stop and record why.
@test:Config {}
function testListenerStopsAfterMalformedCdcEventFailsNormalization() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 3}, reconnectRetry: {maxRetries: 0}}
    });
    FixtureCountingService handler = new;
    check endpoint.attach(handler, FIXTURE_MALFORMED_CDC_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    test:assertTrue(endpoint.getLastError() is ListenerError);
    test:assertEquals(handler.deliveryCount(), 0);
    byte[]? replayId = check replayStore.load({
        tenantId: "00DFixture000001",
        topic: FIXTURE_MALFORMED_CDC_TOPIC,
        subscriptionName: "default"
    });
    test:assertTrue(replayId is (), "a never-delivered event must not be checkpointed");
    check endpoint.immediateStop();
}

// This fails if a CDC decode/normalization failure on one topic leaves other
// attached topics running, unlike an ordinary transport failure (already
// proven terminal listener-wide by testListenerStopsAllTopicsWhenOneFailsTerminally).
@test:Config {}
function testListenerStopsAllTopicsWhenCdcNormalizationFailsOnOneTopic() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {reconnectRetry: {maxRetries: 0}}
    });
    Service healthyHandler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    FixtureCountingService cdcHandler = new;
    check endpoint.attach(healthyHandler, FIXTURE_TOPIC);
    check endpoint.attach(cdcHandler, FIXTURE_MALFORMED_CDC_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    ListenerError? lastError = endpoint.getLastError();
    test:assertTrue(lastError is ListenerError);
    if lastError is ListenerError {
        test:assertEquals(lastError.topic, FIXTURE_MALFORMED_CDC_TOPIC);
    }
    check endpoint.immediateStop();
}

// This fails if decoding an event against a writer schema that does not
// match its actual bytes (Salesforce serving a stale/incorrect schema, as
// distinct from a malformed-but-schema-conformant CDC bitmap) is not treated
// as a normal terminal Subscribe failure.
@test:Config {}
function testListenerTreatsMismatchedCdcWriterSchemaAsTerminal() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {reconnectRetry: {maxRetries: 0}}
    });
    FixtureCountingService handler = new;
    check endpoint.attach(handler, FIXTURE_INVALID_CDC_SCHEMA_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    test:assertTrue(endpoint.getLastError() is ListenerError);
    test:assertEquals(handler.deliveryCount(), 0);
    check endpoint.immediateStop();
}

// This fails if CREATE, UPDATE, and DELETE change events are not each
// normalized correctly over the real (now-fixed) Avro decode path: a
// CREATE/UPDATE with Name in changedFields must surface it in changedData,
// while a DELETE (nothing changed) must not.
@test:Config {}
function testListenerNormalizesCreateUpdateDeleteChangeEventsOverTheWire() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore
    });
    FixtureCdcRecorderService handler = new;
    check endpoint.attach(handler, FIXTURE_CDC_EVENTS_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    check endpoint.immediateStop();

    test:assertEquals(handler.deliveries(), ["CREATE:Acme Inc", "UPDATE:Acme International", "DELETE:<absent>"]);
}

// This fails if the connector resolves/decodes a batch's events using
// whichever writer schema was fetched first, instead of each event's own
// recorded schema ID -- the actual behavior schema evolution depends on. Two
// events land in the same FetchResponse, encoded against two different
// schema versions and each tagged with its own distinct schema ID.
@test:Config {}
function testListenerDecodesEachEventWithItsOwnRecordedSchema() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore
    });
    FixtureSchemaEvolutionRecorderService handler = new;
    check endpoint.attach(handler, FIXTURE_SCHEMA_EVOLUTION_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    check endpoint.immediateStop();

    test:assertEquals(handler.deliveries(), ["old-shape:<absent>", "new-shape:added-field"]);
}

// This fails if Salesforce rejecting a stored replay cursor as invalid or
// expired does not fall back to the configured `expiredReplayRecovery`
// position (EARLIEST by default) on a fresh stream, per D15.
@test:Config {}
function testListenerRecoversFromRejectedReplayUsingConfiguredPolicy() returns error? {
    InMemoryReplayStore replayStore = new;
    ReplayKey checkpointKey = {
        tenantId: "00DFixture000001",
        topic: FIXTURE_REPLAY_RECOVERY_TOPIC,
        subscriptionName: "default"
    };
    // Seed a stale cursor so the first connection attempt uses CUSTOM and the
    // fixture rejects it, forcing the recovery path.
    check replayStore.save(checkpointKey, [99, 99, 99]);

    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {
            handlerRetry: {maxRetries: 0},
            reconnectRetry: {maxRetries: 1, initialDelay: 0.05, maxDelay: 0.05},
            expiredReplayRecovery: EARLIEST
        }
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    check endpoint.attach(handler, FIXTURE_REPLAY_RECOVERY_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    check endpoint.immediateStop();
    byte[]? replayId = check replayStore.load(checkpointKey);
    test:assertEquals(replayId, [11, 12, 13]);
}

// This fails if one topic's terminal failure does not stop every other
// attached topic on the same Listener, per D17.
@test:Config {}
function testListenerStopsAllTopicsWhenOneFailsTerminally() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    // FIXTURE_TOPIC's stream stays open (healthy); FIXTURE_ERROR_TOPIC fails
    // immediately and must take the whole Listener down with it. (Not
    // FIXTURE_PERMISSION_DENIED_TOPIC: its attempt counter is a plain
    // module-level variable shared with testListenerTreatsPermissionDeniedAsTerminalWithoutRetry,
    // so reusing that topic here would leak count across tests.)
    Service healthyHandler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    Service failingHandler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    check endpoint.attach(healthyHandler, FIXTURE_TOPIC);
    check endpoint.attach(failingHandler, FIXTURE_ERROR_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.3);
    ListenerError? lastError = endpoint.getLastError();
    test:assertTrue(lastError is ListenerError);
    if lastError is ListenerError {
        test:assertEquals(lastError.topic, FIXTURE_ERROR_TOPIC);
    }
    // The healthy topic's stream must have been closed too: stopping again
    // must be a clean no-op rather than erroring on stale state.
    check endpoint.immediateStop();
}

// This fails if two topics sharing one Listener do not each track their own
// replay cursor independently -- for example if a shared/reused piece of
// state let one topic's progress overwrite or block the other's -- and if a
// deliberately slower handler on one topic ever holds up the other topic's
// concurrent delivery and checkpointing.
@test:Config {}
function testListenerMakesIndependentProgressAcrossTwoTopics() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore
    });
    Service slowHandler = service object {
        remote function onEvent(Event event) returns error? {
            // Long enough to still be in progress while the other topic's
            // fast handler finishes and checkpoints, if they were wrongly
            // serialized against each other instead of running concurrently.
            runtime:sleep(0.3);
            return ();
        }
    };
    FixtureCountingService fastHandler = new;
    check endpoint.attach(slowHandler, FIXTURE_TOPIC);
    check endpoint.attach(fastHandler, FIXTURE_MULTI_EVENT_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.6);
    check endpoint.immediateStop();

    test:assertEquals(fastHandler.deliveryCount(), 10);
    byte[]? slowTopicReplayId = check replayStore.load({
        tenantId: "00DFixture000001", topic: FIXTURE_TOPIC, subscriptionName: "default"
    });
    byte[]? fastTopicReplayId = check replayStore.load({
        tenantId: "00DFixture000001", topic: FIXTURE_MULTI_EVENT_TOPIC, subscriptionName: "default"
    });
    test:assertEquals(slowTopicReplayId, [7, 8, 9]);
    test:assertEquals(fastTopicReplayId, [<byte>10]);
}

// This fails if a topic that fails to open (here, an unrecognized topic name)
// leaves an earlier, already-opened topic's stream dangling instead of being
// cleaned up as part of 'start() surfacing the failure -- proven by a
// subsequent stop being a safe no-op rather than erroring on leaked state.
@test:Config {}
function testListenerCleansUpAlreadyOpenedTopicsWhenAnotherFailsToOpen() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    check endpoint.attach(handler, FIXTURE_TOPIC);
    check endpoint.attach(handler, "/event/FixtureNotRegisteredWithFixtureServer__e");
    error? startError = endpoint.'start();

    test:assertTrue(startError is error);
    test:assertTrue(endpoint.getLastError() is (), "a start-time failure is not the same as a post-start terminal failure");
    // If the first topic's stream were left open, this would either error or
    // there would be nothing meaningful left to clean up safely twice.
    check endpoint.immediateStop();
    check endpoint.immediateStop();
}

// This fails if calling gracefulStop/immediateStop more than once errors or
// double-closes state instead of being a safe no-op.
@test:Config {}
function testListenerRepeatedStopIsIdempotent() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            return ();
        }
    };
    check endpoint.attach(handler, FIXTURE_TOPIC);
    check endpoint.'start();
    runtime:sleep(0.2);
    check endpoint.immediateStop();
    check endpoint.immediateStop();
    check endpoint.gracefulStop();
}
