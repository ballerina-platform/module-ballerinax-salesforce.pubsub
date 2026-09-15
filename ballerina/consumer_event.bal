// Copyright (c) 2026, WSO2 LLC. All rights reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.

import ballerinax/salesforce.pubsub.internal as wire;

# Handles one decoded event. The public Listener service adapter will implement
# this internal boundary without exposing generated wire records to users.
type EventHandler distinct object {
    public function onEvent(Event event) returns error?;
};

# Converts one generated Pub/Sub event to the public dynamic envelope using
# the writer schema named by that event.
#
# + topic - canonical service-path topic that owns the stream
# + schemaJson - writer schema resolved for `wireEvent.event.schema_id`
# + wireEvent - generated protobuf event kept inside the connector boundary
# + return - decoded public event or a schema/payload decoding error
isolated function eventFromConsumerEvent(string topic, string schemaJson, wire:ConsumerEvent wireEvent) returns Event|error {
    Payload payload = check decodePayload(schemaJson, wireEvent.event.payload);
    if topic.startsWith("/data/") {
        payload = check normalizeCdcPayload(schemaJson, payload);
    }
    Header[] headers = [];
    foreach wire:EventHeader header in wireEvent.event.headers {
        headers.push({key: header.key, value: header.value.clone()});
    }
    string? eventId = wireEvent.event.id.length() > 0 ? wireEvent.event.id : ();
    return {
        topic,
        replayId: wireEvent.replay_id.clone(),
        schemaId: wireEvent.event.schema_id,
        eventId,
        headers,
        payload
    };
}

// Delivers exactly one event in sequential order. A durable replay save occurs
// only after a successful handler result; either failure leaves the gate closed
// for retry or recovery and never permits a later event to pass it.
function deliverConsumerEvent(string topic, string schemaJson, wire:ConsumerEvent wireEvent, ReplayKey replayKey,
        ReplayStore replayStore, EventHandler handler, DeliveryGate gate) returns Event|error {
    Event event = check eventFromConsumerEvent(topic, schemaJson, wireEvent);
    return deliverDecodedEvent(event, replayKey, replayStore, handler, gate, true);
}

// Retries the event already held by the DeliveryGate. Callers must first move
// the gate from the failure state with `retryHandler`.
function retryConsumerEvent(Event event, ReplayKey replayKey, ReplayStore replayStore, EventHandler handler,
        DeliveryGate gate) returns Event|error {
    return deliverDecodedEvent(event, replayKey, replayStore, handler, gate, false);
}

function deliverDecodedEvent(Event event, ReplayKey replayKey, ReplayStore replayStore, EventHandler handler,
        DeliveryGate gate, boolean beginDelivery) returns Event|error {
    if beginDelivery {
        check gate.beginEvent();
    }
    error? handlerError = handler.onEvent(event);
    if handlerError is error {
        gate.handlerFailed();
        return handlerError;
    }
    check gate.handlerSucceeded();
    error? saveError = replayStore.save(replayKey, event.replayId);
    if saveError is error {
        gate.checkpointFailed();
        return saveError;
    }
    check gate.checkpointSaved();
    return event;
}
