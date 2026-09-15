// Copyright (c) 2026, WSO2 LLC. All rights reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.

// The Avro runtime needs concrete primitive-array types, whereas values in an
// open Payload record can reach it as untyped anydata arrays. Normalize the
// schema-declared array fields before encoding so callers retain the public
// dynamic-Payload API.
isolated function normalizePayloadForAvro(string schemaJson, Payload payload) returns Payload|error {
    json rootJson = check schemaJson.fromJsonString();
    map<json> root = check rootJson.ensureType();
    map<anydata> normalized = check normalizeRecordFields(root, payload);
    return normalized;
}

// Recurses into nested record fields so their own array-typed sub-fields are
// normalized too. An array nested inside a sub-record (for example CDC's
// ChangeEventHeader.changedFields/nulledFields/diffFields) needs exactly the
// same primitive-array coercion as a top-level array field; only handling the
// top level here previously left nested arrays untyped, which the underlying
// Avro encoder cannot serialize.
isolated function normalizeRecordFields(map<json> recordSchema, map<anydata> payload) returns map<anydata>|error {
    json[] fields = check recordSchema["fields"].ensureType();
    map<anydata> normalized = payload.clone();
    foreach json fieldJson in fields {
        map<json> schemaField = check fieldJson.ensureType();
        string name = check schemaField["name"].ensureType();
        anydata? value = payload[name];
        if value is () {
            continue;
        }
        normalized[name] = check normalizeAvroValue(schemaField["type"], value);
    }
    return normalized;
}

isolated function normalizeAvroValue(json? avroType, anydata value) returns anydata|error {
    if avroType is json[] {
        // Select the first non-null branch compatible with a present value.
        foreach json branch in avroType {
            if branch is string && branch == "null" {
                continue;
            }
            return normalizeAvroValue(branch, value);
        }
        return value;
    }
    if avroType !is map<json> {
        return value;
    }
    string kind = check avroType["type"].ensureType();
    if kind == "record" {
        map<anydata> nestedValue = check value.ensureType();
        return check normalizeRecordFields(avroType, nestedValue);
    }
    if kind != "array" {
        return value;
    }
    json items = check avroType["items"].ensureType();
    string? itemKind = items is string ? items : ();
    anydata[] values = check value.cloneWithType();
    if itemKind == "string" {
        string[] normalized = [];
        foreach anydata item in values {
            string typedItem = check item.ensureType();
            normalized.push(typedItem);
        }
        return normalized;
    }
    if itemKind == "int" || itemKind == "long" {
        int[] normalized = [];
        foreach anydata item in values {
            int typedItem = check item.ensureType();
            normalized.push(typedItem);
        }
        return normalized;
    }
    if itemKind == "boolean" {
        boolean[] normalized = [];
        foreach anydata item in values {
            boolean typedItem = check item.ensureType();
            normalized.push(typedItem);
        }
        return normalized;
    }
    if itemKind == "float" || itemKind == "double" {
        float[] normalized = [];
        foreach anydata item in values {
            float typedItem = check item.ensureType();
            normalized.push(typedItem);
        }
        return normalized;
    }
    return value;
}
