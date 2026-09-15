import ballerina/test;
import ballerinax/salesforce.pubsub.internal as wire;

@test:Config {}
function testPublisherBuildsWireEventsFromWriterSchema() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"Name\",\"type\":\"string\"}]}";
    wire:ProducerEvent[] wireEvents = check producerEventsFor(schema, "schema-1", [
        {id: "event-1", payload: {"Name": "Acme"}}
    ]);
    test:assertEquals(wireEvents.length(), 1);
    test:assertEquals(wireEvents[0].id, "event-1");
    test:assertEquals(wireEvents[0].schema_id, "schema-1");
    test:assertTrue(wireEvents[0].payload.length() > 0);
}

// Local Avro failures must not discard valid sibling events before Publish.
@test:Config {}
function testPublisherKeepsValidEventsWhenSiblingEncodingFails() {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"Status\",\"type\":{\"type\":\"enum\",\"name\":\"StatusEnum\",\"symbols\":[\"NEW\"]}}]}";
    PreparedPublishEvents prepared = prepareProducerEvents(schema, "schema-1", [
        {id: "valid", payload: {"Status": "NEW"}},
        {id: "invalid", payload: {"Status": "OLD"}}
    ]);

    test:assertEquals(prepared.wireEvents.length(), 1);
    test:assertEquals(prepared.wireEvents[0].id, "valid");
    test:assertEquals(prepared.localFailures.length(), 1);
    test:assertEquals(prepared.localFailures[0].id, "invalid");
    test:assertTrue(prepared.localFailures[0].itemError is error);
}

// This connector does not independently enforce Salesforce's size limits, but
// its own request-splitting target still depends on this exact wire-size
// accounting, so the tag/varint overhead it counts is pinned here.
@test:Config {}
function testPublisherCountsProtobufWireOverheadForSizeLimits() {
    wire:ProducerEvent event = {id: "id", schema_id: "s", payload: [1, 2]};
    test:assertEquals(producerEventWireSize(event), 11);
}

function fixtureWireEvent(string id) returns wire:ProducerEvent => {id, schema_id: "s", payload: [1, 2]};

// Each of these events contributes 13 bytes to a request ("/x" itself
// contributes a fixed 4), matching testPublisherCountsProtobufWireOverheadForSizeLimits.
// This fails if splitting either ignores the size target or drops/reorders events.
@test:Config {}
function testChunkProducerEventsSplitsAtSizeTarget() {
    wire:ProducerEvent[] events = [fixtureWireEvent("e1"), fixtureWireEvent("e2"), fixtureWireEvent("e3")];
    // Room for exactly two events per chunk (4 + 13 + 13 = 30); a third would
    // reach 43.
    wire:ProducerEvent[][] chunks = chunkProducerEvents("/x", events, 30);
    test:assertEquals(chunks.length(), 2);
    test:assertEquals(chunks[0].length(), 2);
    test:assertEquals(chunks[0][0].id, "e1");
    test:assertEquals(chunks[0][1].id, "e2");
    test:assertEquals(chunks[1].length(), 1);
    test:assertEquals(chunks[1][0].id, "e3");
}

// A single event whose own contribution already reaches the target must
// still get sent, alone, rather than being dropped or blocking every other
// event from being chunked at all.
@test:Config {}
function testChunkProducerEventsGivesAnOversizedSingleEventItsOwnChunk() {
    wire:ProducerEvent[] events = [fixtureWireEvent("e1"), fixtureWireEvent("e2")];
    wire:ProducerEvent[][] chunks = chunkProducerEvents("/x", events, 5);
    test:assertEquals(chunks.length(), 2);
    test:assertEquals(chunks[0].length(), 1);
    test:assertEquals(chunks[0][0].id, "e1");
    test:assertEquals(chunks[1].length(), 1);
    test:assertEquals(chunks[1][0].id, "e2");
}

@test:Config {}
function testChunkProducerEventsReturnsNoChunksForNoEvents() {
    wire:ProducerEvent[][] chunks = chunkProducerEvents("/x", [], 1024);
    test:assertEquals(chunks.length(), 0);
}
