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

isolated class RecordingEventHandler {
    *EventHandler;
    private int calls = 0;

    public isolated function onEvent(Event event) returns error? {
        lock {
            self.calls += 1;
        }
    }

    isolated function callCount() returns int {
        lock {
            return self.calls;
        }
    }
}

isolated class FailingEventHandler {
    *EventHandler;

    public isolated function onEvent(Event event) returns error? {
        return error("application failure");
    }
}

isolated class FailsOnceEventHandler {
    *EventHandler;
    private int calls = 0;

    public isolated function onEvent(Event event) returns error? {
        lock {
            self.calls += 1;
            if self.calls == 1 {
                return error("temporary application failure");
            }
        }
    }
}

isolated class FailingReplayStore {
    *ReplayStore;

    public isolated function load(ReplayKey key) returns byte[]|error? {
        return ();
    }

    public isolated function save(ReplayKey key, byte[] replayId) returns error? {
        return error("checkpoint unavailable");
    }
}

isolated class FailsReplaySevenHandler {
    *EventHandler;

    public isolated function onEvent(Event event) returns error? {
        if event.replayId[0] == 7 {
            return error("event seven failed");
        }
    }
}

// A successful handler must save the exact replay bytes before the delivery
// gate permits later events or replacement flow-control capacity.
@test:Config {}
function testConsumerDeliveryCheckpointsOnlyAfterSuccessfulHandler() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    byte[] payload = check encodePayload(schema, {});
    InMemoryReplayStore store = new;
    RecordingEventHandler handler = new;
    DeliveryGate gate = new;
    ReplayKey key = {tenantId: "00D000000000001", topic: "/event/Order__e", subscriptionName: "orders"};

    _ = check deliverConsumerEvent("/event/Order__e", schema, {
        event: {schema_id: "schema-1", payload}, replay_id: [7, 8]
    }, key, store, handler, gate);

    test:assertEquals(handler.callCount(), 1);
    test:assertEquals(check store.load(key), [7, 8]);
    test:assertTrue(gate.canDeliverNext());
}

// A failed handler must leave the durable checkpoint unchanged and block later
// work until retry/recovery takes responsibility for the failed event.
@test:Config {}
function testConsumerDeliveryLeavesCheckpointOnHandlerFailure() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    byte[] payload = check encodePayload(schema, {});
    InMemoryReplayStore store = new;
    FailingEventHandler handler = new;
    DeliveryGate gate = new;
    ReplayKey key = {tenantId: "00D000000000001", topic: "/event/Order__e", subscriptionName: "orders"};

    Event|error result = deliverConsumerEvent("/event/Order__e", schema, {
        event: {schema_id: "schema-1", payload}, replay_id: [9]
    }, key, store, handler, gate);

    test:assertTrue(result is error);
    test:assertEquals(check store.load(key), ());
    test:assertFalse(gate.canDeliverNext());
}

// A replay-store failure after a successful handler must leave the gate closed;
// otherwise later work could move a checkpoint beyond the unresolved event.
@test:Config {}
function testConsumerDeliveryBlocksAfterCheckpointFailure() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    byte[] payload = check encodePayload(schema, {});
    FailingReplayStore store = new;
    RecordingEventHandler handler = new;
    DeliveryGate gate = new;
    ReplayKey key = {tenantId: "00D000000000001", topic: "/event/Order__e", subscriptionName: "orders"};

    Event|error result = deliverConsumerEvent("/event/Order__e", schema, {
        event: {schema_id: "schema-1", payload}, replay_id: [10]
    }, key, store, handler, gate);

    test:assertTrue(result is error);
    test:assertEquals(handler.callCount(), 1);
    test:assertFalse(gate.canDeliverNext());
    test:assertFalse(gate.canReplenish());
}

// Retrying a failed event reuses the same envelope and gate. It checkpoints
// only after the retry succeeds, so later buffered events never pass it.
@test:Config {}
function testConsumerDeliveryCanRetryTheSameFailedEvent() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    byte[] payload = check encodePayload(schema, {});
    InMemoryReplayStore store = new;
    FailsOnceEventHandler handler = new;
    DeliveryGate gate = new;
    ReplayKey key = {tenantId: "00D000000000001", topic: "/event/Order__e", subscriptionName: "orders"};
    wire:ConsumerEvent wireEvent = {event: {schema_id: "schema-1", payload}, replay_id: [11]};

    Event|error first = deliverConsumerEvent("/event/Order__e", schema, wireEvent, key, store, handler, gate);
    test:assertTrue(first is error);
    check gate.retryHandler();
    Event event = check eventFromConsumerEvent("/event/Order__e", schema, wireEvent);
    _ = check retryConsumerEvent(event, key, store, handler, gate);

    test:assertEquals(check store.load(key), [11]);
    test:assertTrue(gate.canDeliverNext());
}

// Acceptance case: after event 6 is checkpointed, event 7 must fail without
// advancing the cursor. A reconnect therefore resumes after event 6 and makes
// event 7 replayable rather than silently skipping it.
@test:Config {}
function testEventSevenFailureLeavesCheckpointAtEventSix() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    byte[] payload = check encodePayload(schema, {});
    InMemoryReplayStore store = new;
    FailsReplaySevenHandler handler = new;
    DeliveryGate gate = new;
    ReplayKey key = {tenantId: "00D000000000001", topic: "/event/Order__e", subscriptionName: "orders"};

    _ = check deliverConsumerEvent("/event/Order__e", schema, {
        event: {schema_id: "schema-1", payload}, replay_id: [6]
    }, key, store, handler, gate);
    Event|error eventSeven = deliverConsumerEvent("/event/Order__e", schema, {
        event: {schema_id: "schema-1", payload}, replay_id: [7]
    }, key, store, handler, gate);

    test:assertTrue(eventSeven is error);
    test:assertEquals(check store.load(key), [6]);
    test:assertFalse(gate.canDeliverNext());
}
