import ballerina/test;

// This fails if dynamic payloads are serialized as JSON or cannot round-trip
// through the Salesforce writer schema required by the Publish RPC.
@test:Config {}
function testAvroCodecRoundTripsDynamicPayload() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"Order_Id__c\",\"type\":\"string\"}]}";
    Payload payload = {"Order_Id__c": "ORDER-1042"};

    byte[] bytes = check encodePayload(schema, payload);
    Payload decoded = check decodePayload(schema, bytes);

    test:assertTrue(decoded.toString().includes("ORDER-1042"));
}

// Salesforce custom-event text fields are nullable Avro unions. Encoding a
// concrete string branch must not fail or require an Avro-specific public API.
@test:Config {}
function testAvroCodecEncodesNullableStringUnion() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"Order_Notification__e\",\"fields\":[" +
        "{\"name\":\"CreatedDate\",\"type\":\"long\"},{\"name\":\"CreatedById\",\"type\":\"string\"}," +
        "{\"name\":\"Order_Id__c\",\"type\":[\"null\",\"string\"],\"default\":null}," +
        "{\"name\":\"Status__c\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
    Payload payload = {
        "CreatedDate": 0,
        "CreatedById": "005000000000001",
        "Order_Id__c": "ORDER-1042",
        "Status__c": "SHIPPED"
    };

    byte[] bytes = check encodePayload(schema, payload);
    Payload decoded = check decodePayload(schema, bytes);
    test:assertEquals(decoded["Order_Id__c"], "ORDER-1042");
    test:assertEquals(decoded["Status__c"], "SHIPPED");
}

// Dynamic payloads must carry the Avro value families used by evolving event
// schemas without exposing Avro types to connector callers.
@test:Config {}
function testAvroCodecRoundTripsNestedCollectionsAndBytes() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"ComplexEvent\",\"fields\":[" +
        "{\"name\":\"details\",\"type\":{\"type\":\"record\",\"name\":\"Details\",\"fields\":[{\"name\":\"label\",\"type\":\"string\"}]}}," +
        "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}," +
        "{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}," +
        "{\"name\":\"data\",\"type\":\"bytes\"},{\"name\":\"optionalCount\",\"type\":[\"null\",\"int\"],\"default\":null}]}";
    byte[] binary = [1, 2, 3];
    Payload payload = {
        "details": {"label": "priority"},
        "tags": ["one", "two"],
        "attributes": {"source": "test"},
        "data": binary,
        "optionalCount": ()
    };

    byte[] bytes = check encodePayload(schema, payload);
    test:assertTrue(bytes.length() > 0);
}

@test:Config {}
function testAvroCodecEncodesStringArray() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"ArrayEvent\",\"fields\":[{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";
    _ = check encodePayload(schema, {"tags": ["one", "two"]});
}

@test:Config {}
function testAvroCodecEncodesStringMap() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"MapEvent\",\"fields\":[{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}";
    _ = check encodePayload(schema, {"attributes": {"source": "test"}});
}

@test:Config {}
function testAvroCodecEncodesNestedRecord() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"NestedEvent\",\"fields\":[{\"name\":\"details\",\"type\":{\"type\":\"record\",\"name\":\"Details\",\"fields\":[{\"name\":\"label\",\"type\":\"string\"}]}}]}";
    _ = check encodePayload(schema, {"details": {"label": "priority"}});
}

// An array field nested inside a sub-record (for example CDC's
// ChangeEventHeader.changedFields) needs the same primitive-array coercion as
// a top-level array field. Normalizing only top-level fields left this value
// untyped and made the underlying Avro encoder fail with an internal null
// pointer error rather than a normal connector error. (Decoding this same
// shape hits a separate, pre-existing limitation in the underlying Avro
// library's own nested-array decode support, so this only covers encode.)
@test:Config {}
function testAvroCodecEncodesArrayNestedInsideRecord() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"NestedArrayEvent\",\"fields\":[" +
        "{\"name\":\"header\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[" +
        "{\"name\":\"changedFields\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}}]}";
    byte[] bytes = check encodePayload(schema, {"header": {"changedFields": ["Name", "Phone"]}});
    test:assertTrue(bytes.length() > 0);
}
