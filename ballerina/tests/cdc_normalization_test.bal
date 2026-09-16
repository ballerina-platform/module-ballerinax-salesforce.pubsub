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

// CDC bitmap positions refer to the Avro writer schema. The first top-level
// field is ChangeEventHeader, so 0x06 expands positions 1 and 2 to Name/Phone.
@test:Config {}
function testCdcNormalizationExpandsBitmapsAndKeepsExplicitNulls() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[]}}," +
        "{\"name\":\"Name\",\"type\":[\"null\",\"string\"]}," +
        "{\"name\":\"Phone\",\"type\":[\"null\",\"string\"]}]}";
    Payload raw = {
        "ChangeEventHeader": {
            "entityName": "Account",
            "changeType": "UPDATE",
            "recordIds": ["001"],
            "changedFields": ["0x06"],
            "nulledFields": ["0x04"],
            "diffFields": []
        },
        "Name": "Acme International",
        "Phone": ()
    };

    Payload normalized = check normalizeCdcPayload(schema, raw);
    map<anydata> changedData = check normalized["changedData"].ensureType();
    map<anydata> metadata = check normalized["metadata"].ensureType();
    test:assertEquals(changedData, {"Name": "Acme International", "Phone": ()});
    test:assertEquals(metadata["entityName"], "Account");
    test:assertEquals(metadata["changedFields"], ["Name", "Phone"]);
    test:assertEquals(metadata["nulledFields"], ["Phone"]);
    test:assertEquals(metadata["diffFields"], []);
}

// Compound bitmap entries use `parentPosition-nestedBitmap` and expose the
// child as a dotted field name while retaining its nested data shape.
@test:Config {}
function testCdcNormalizationExpandsCompoundFieldBitmaps() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[]}}," +
        "{\"name\":\"BillingAddress\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":[" +
        "{\"name\":\"Street\",\"type\":[\"null\",\"string\"]},{\"name\":\"City\",\"type\":[\"null\",\"string\"]}]}}]}";
    Payload raw = {
        "ChangeEventHeader": {"changedFields": ["1-0x02"], "nulledFields": [], "diffFields": []},
        "BillingAddress": {"Street": "1 Market", "City": ()}
    };

    Payload normalized = check normalizeCdcPayload(schema, raw);
    map<anydata> changedData = check normalized["changedData"].ensureType();
    map<anydata> metadata = check normalized["metadata"].ensureType();
    test:assertEquals(metadata["changedFields"], ["BillingAddress.City"]);
    test:assertEquals(changedData, {"BillingAddress": {"City": ()}});
}

// More than one bitmap entry is merged in writer-schema order. A null field
// absent from changedFields remains absent, whereas a changed null is retained.
@test:Config {}
function testCdcNormalizationCombinesEntriesAndExcludesUnchangedNulls() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[]}}," +
        "{\"name\":\"Name\",\"type\":[\"null\",\"string\"]}," +
        "{\"name\":\"Phone\",\"type\":[\"null\",\"string\"]}," +
        "{\"name\":\"Website\",\"type\":[\"null\",\"string\"]}]}";
    Payload raw = {
        "ChangeEventHeader": {"changedFields": ["0x02", "0x08"], "nulledFields": [], "diffFields": ["0x08"]},
        "Name": "Acme International",
        "Phone": (),
        "Website": "https://example.test"
    };

    Payload normalized = check normalizeCdcPayload(schema, raw);
    map<anydata> changedData = check normalized["changedData"].ensureType();
    map<anydata> metadata = check normalized["metadata"].ensureType();
    test:assertEquals(changedData, {"Name": "Acme International", "Website": "https://example.test"});
    test:assertEquals(metadata["changedFields"], ["Name", "Website"]);
    test:assertEquals(metadata["diffFields"], ["Website"]);
}

// Bitmap positions are evaluated against the exact writer schema carried by
// each envelope, so a field added after an older schema remains distinguishable.
@test:Config {}
function testCdcNormalizationUsesWriterSchemaForEvolvedFields() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[]}}," +
        "{\"name\":\"Name\",\"type\":\"string\"},{\"name\":\"NewField__c\",\"type\":\"string\"}]}";
    Payload normalized = check normalizeCdcPayload(schema, {
        "ChangeEventHeader": {"changedFields": ["0x04"], "nulledFields": [], "diffFields": []},
        "Name": "Acme", "NewField__c": "new value"
    });

    map<anydata> changedData = check normalized["changedData"].ensureType();
    test:assertEquals(changedData, {"NewField__c": "new value"});
}

// Bad bitmaps fail before an event reaches an application handler; the
// Listener then follows its ordinary sequential retry and terminal policy.
@test:Config {}
function testCdcNormalizationRejectsMalformedBitmap() {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[]}}]}";
    Payload|error result = normalizeCdcPayload(schema, {
        "ChangeEventHeader": {"changedFields": ["not-a-bitmap"], "nulledFields": [], "diffFields": []}
    });
    test:assertTrue(result is error);
}

// A missing "fields" array (a structurally invalid writer schema, distinct
// from a malformed bitmap value) must fail the same way rather than panicking
// or silently skipping bitmap expansion.
@test:Config {}
function testCdcNormalizationRejectsInvalidSchema() {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\"}";
    Payload|error result = normalizeCdcPayload(schema, {
        "ChangeEventHeader": {"changedFields": ["0x02"], "nulledFields": [], "diffFields": []}
    });
    test:assertTrue(result is error);
}

// A CREATE event's changedFields covers every data field, since the whole
// record is new.
@test:Config {}
function testCdcNormalizationHandlesCreateChangeType() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[]}}," +
        "{\"name\":\"Name\",\"type\":[\"null\",\"string\"]}," +
        "{\"name\":\"Phone\",\"type\":[\"null\",\"string\"]}]}";
    Payload raw = {
        "ChangeEventHeader": {
            "entityName": "Account",
            "changeType": "CREATE",
            "recordIds": ["001"],
            "changedFields": ["0x06"],
            "nulledFields": [],
            "diffFields": []
        },
        "Name": "Acme International",
        "Phone": "555-0100"
    };

    Payload normalized = check normalizeCdcPayload(schema, raw);
    map<anydata> changedData = check normalized["changedData"].ensureType();
    map<anydata> metadata = check normalized["metadata"].ensureType();
    test:assertEquals(changedData, {"Name": "Acme International", "Phone": "555-0100"});
    test:assertEquals(metadata["changeType"], "CREATE");
    test:assertEquals(metadata["changedFields"], ["Name", "Phone"]);
}

// Confirmed against real Salesforce CDC data: a CREATE event's changedFields
// bitmap can come back completely empty even though the record has several
// non-null field values -- every populated field on a new record must still
// be treated as changed, falling back to the writer schema's own top-level
// field list whenever the bitmap comes back empty. A field that is genuinely
// absent (null) is still excluded. This fallback is intentionally gated on
// bitmap-emptiness alone, not ChangeEventHeader.changeType -- see the comment
// in normalizeCdcPayload for why changeType specifically isn't read here.
@test:Config {}
function testCdcNormalizationTreatsAllPopulatedFieldsAsChangedWhenBitmapIsEmpty() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[]}}," +
        "{\"name\":\"Name\",\"type\":[\"null\",\"string\"]}," +
        "{\"name\":\"Phone\",\"type\":[\"null\",\"string\"]}]}";
    Payload raw = {
        "ChangeEventHeader": {
            "entityName": "Account",
            "changeType": "CREATE",
            "recordIds": ["001"],
            "changedFields": [],
            "nulledFields": [],
            "diffFields": []
        },
        "Name": "Acme International",
        "Phone": ()
    };

    Payload normalized = check normalizeCdcPayload(schema, raw);
    map<anydata> changedData = check normalized["changedData"].ensureType();
    map<anydata> metadata = check normalized["metadata"].ensureType();
    test:assertEquals(changedData, {"Name": "Acme International"});
    test:assertEquals(metadata["changedFields"], ["Name"]);
}

// A DELETE event typically carries no changed/nulled/diff field bitmaps at
// all; normalization must produce empty changedData rather than erroring on
// the absence of field-level data.
@test:Config {}
function testCdcNormalizationHandlesDeleteChangeType() returns error? {
    string schema = "{\"type\":\"record\",\"name\":\"AccountChangeEvent\",\"fields\":[" +
        "{\"name\":\"ChangeEventHeader\",\"type\":{\"type\":\"record\",\"name\":\"Header\",\"fields\":[]}}," +
        "{\"name\":\"Name\",\"type\":[\"null\",\"string\"]}]}";
    Payload raw = {
        "ChangeEventHeader": {
            "entityName": "Account",
            "changeType": "DELETE",
            "recordIds": ["001"],
            "changedFields": [],
            "nulledFields": [],
            "diffFields": []
        },
        "Name": ()
    };

    Payload normalized = check normalizeCdcPayload(schema, raw);
    map<anydata> changedData = check normalized["changedData"].ensureType();
    map<anydata> metadata = check normalized["metadata"].ensureType();
    test:assertEquals(changedData, {});
    test:assertEquals(metadata["changeType"], "DELETE");
    test:assertEquals(metadata["changedFields"], []);
}
