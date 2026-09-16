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

import ballerina/test;
import ballerinax/salesforce.pubsub.internal as wire;

// Consumer envelopes preserve the event schema, opaque replay bytes, optional
// event ID, and repeated binary headers while decoding the Avro payload.
@test:Config {}
function testConsumerEventMapsWireEnvelopeWithoutLosingHeaders() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"Name\",\"type\":\"string\"}]}";
    byte[] payload = check encodePayload(schema, {"Name": "Acme"});
    wire:ConsumerEvent wireEvent = {
        event: {
            id: "event-42",
            schema_id: "schema-42",
            payload,
            headers: [{key: "trace", value: [1]}, {key: "trace", value: [2]}]
        },
        replay_id: [9, 8, 7]
    };

    Event event = check eventFromConsumerEvent("/event/Order__e", schema, wireEvent);
    test:assertEquals(event.topic, "/event/Order__e");
    test:assertEquals(event.schemaId, "schema-42");
    test:assertEquals(event.eventId, "event-42");
    test:assertEquals(event.replayId, [9, 8, 7]);
    test:assertEquals(event.headers, [{key: "trace", value: [1]}, {key: "trace", value: [2]}]);
    test:assertEquals(event.payload, {"Name": "Acme"});
}

// CDC and other event families may omit an event ID; the empty wire field must
// remain absent in the public envelope rather than becoming a fake ID.
@test:Config {}
function testConsumerEventKeepsMissingWireIdAbsent() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    byte[] payload = check encodePayload(schema, {});
    Event event = check eventFromConsumerEvent("/data/ChangeEvents", schema, {
        event: {schema_id: "schema-cdc", payload}, replay_id: [1]
    });

    test:assertEquals(event.eventId, ());
}

// The consumer delivery boundary applies CDC normalization only to /data
// topics. Bitmap expansion itself is covered with decoded CDC fixtures above.
@test:Config {}
function testConsumerEventNormalizesCdcPayloadBeforeDelivery() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[" +
        "{\"name\":\"entityName\",\"type\":\"string\"}]}}," +
        "{\"name\":\"Name\",\"type\":\"string\"}]}";
    byte[] payload = check encodePayload(schema, {
        "ChangeEventHeader": {"entityName": "Account"},
        "Name": "Acme International"
    });

    Event event = check eventFromConsumerEvent("/data/AccountChangeEvent", schema, {
        event: {schema_id: "schema-cdc", payload}, replay_id: [1]
    });
    test:assertEquals(event.payload["changedData"], {});
    map<anydata> metadata = check event.payload["metadata"].ensureType();
    test:assertEquals(metadata["entityName"], "Account");
}

// Real-time event monitoring events are delivered as ordinary platform events
// (no `/data/` prefix), so this family must decode plainly like any other
// platform event rather than being mistaken for CDC and CDC-normalized.
@test:Config {}
function testConsumerEventDecodesMonitoringEventWithoutCdcNormalization() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"ApiEvent\",\"fields\":[" +
        "{\"name\":\"EventIdentifier\",\"type\":\"string\"},{\"name\":\"UserId\",\"type\":\"string\"}]}";
    byte[] payload = check encodePayload(schema, {"EventIdentifier": "0000ABCD", "UserId": "005XX"});

    Event event = check eventFromConsumerEvent("/event/ApiEvent", schema, {
        event: {schema_id: "schema-monitoring", payload}, replay_id: [1]
    });
    test:assertEquals(event.payload, {"EventIdentifier": "0000ABCD", "UserId": "005XX"});
    test:assertEquals(event.payload["changedData"], ());
}
