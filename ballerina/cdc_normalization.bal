// Copyright (c) 2026, WSO2 LLC. All rights reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.

import ballerina/lang.'int as intLang;

type CdcSchemaIndex record {|
    string[] topLevelFields;
    map<string[]> compoundFields;
|};

# Converts a decoded Change Data Capture payload to the public `changedData` /
# `metadata` shape. Salesforce stores changed, nulled, and diff field names as
# Avro-schema bitmaps in ChangeEventHeader; those raw bitmaps never escape this
# function.
#
# + schemaJson - writer schema supplied by the consumer event envelope
# + payload - decoded CDC event body
# + return - normalized CDC data and metadata, or a schema/bitmap error
isolated function normalizeCdcPayload(string schemaJson, Payload payload) returns Payload|error {
    anydata? headerValue = payload["ChangeEventHeader"];
    if headerValue !is map<anydata> {
        return payload;
    }
    CdcSchemaIndex schemaIndex = check cdcSchemaIndex(schemaJson);
    string[] changedFields = check expandCdcBitmap(headerValue["changedFields"], schemaIndex);
    string[] nulledFields = check expandCdcBitmap(headerValue["nulledFields"], schemaIndex);
    string[] diffFields = check expandCdcBitmap(headerValue["diffFields"], schemaIndex);

    map<anydata> changedData = {};
    foreach string path in changedFields {
        check addChangedValue(changedData, payload, path);
    }
    map<anydata> metadata = headerValue.clone();
    metadata["changedFields"] = changedFields;
    metadata["nulledFields"] = nulledFields;
    metadata["diffFields"] = diffFields;
    return {"changedData": changedData, "metadata": metadata};
}

isolated function cdcSchemaIndex(string schemaJson) returns CdcSchemaIndex|error {
    json schema = check schemaJson.fromJsonString();
    map<json> root = check schema.ensureType();
    json[] fields = check root["fields"].ensureType();
    string[] topLevelFields = [];
    map<string[]> compoundFields = {};
    foreach json fieldValue in fields {
        map<json> schemaField = check fieldValue.ensureType();
        string name = check schemaField["name"].ensureType();
        topLevelFields.push(name);
        string[] nested = check nestedFieldNames(schemaField["type"]);
        if nested.length() > 0 {
            compoundFields[name] = nested;
        }
    }
    return {topLevelFields, compoundFields};
}

isolated function nestedFieldNames(json? avroType) returns string[]|error {
    if avroType is json[] {
        foreach json member in avroType {
            string[] nested = check nestedFieldNames(member);
            if nested.length() > 0 {
                return nested;
            }
        }
        return [];
    }
    if avroType !is map<json> {
        return [];
    }
    string kind = check avroType["type"].ensureType();
    if kind != "record" {
        return [];
    }
    json[] fields = check avroType["fields"].ensureType();
    string[] names = [];
    foreach json fieldValue in fields {
        map<json> schemaField = check fieldValue.ensureType();
        string nestedName = check schemaField["name"].ensureType();
        names.push(nestedName);
    }
    return names;
}

isolated function expandCdcBitmap(anydata? rawEntries, CdcSchemaIndex schemaIndex) returns string[]|error {
    if rawEntries is () {
        return [];
    }
    string[] entries = check rawEntries.cloneWithType();
    string[] expanded = [];
    foreach string entry in entries {
        string[] parts = re `-`.split(entry);
        if parts.length() == 1 {
            foreach int position in check bitmapPositions(parts[0]) {
                if position < schemaIndex.topLevelFields.length() {
                    expanded.push(schemaIndex.topLevelFields[position]);
                }
            }
            continue;
        }
        if parts.length() != 2 {
            return error("invalid CDC compound bitmap entry");
        }
        int parentPosition = check intLang:fromString(parts[0]);
        if parentPosition < 0 || parentPosition >= schemaIndex.topLevelFields.length() {
            return error("CDC compound bitmap parent position is outside the schema");
        }
        string parent = schemaIndex.topLevelFields[parentPosition];
        string[]? nested = schemaIndex.compoundFields[parent];
        if nested !is string[] {
            return error("CDC compound bitmap references a non-compound field");
        }
        foreach int position in check bitmapPositions(parts[1]) {
            if position < nested.length() {
                expanded.push(parent + "." + nested[position]);
            }
        }
    }
    return expanded;
}

isolated function bitmapPositions(string bitmap) returns int[]|error {
    if !bitmap.startsWith("0x") && !bitmap.startsWith("0X") {
        return error("CDC bitmap must start with 0x");
    }
    int[] positions = [];
    int offset = 0;
    int index = bitmap.length() - 1;
    while index >= 2 {
        int nibble = check hexValue(bitmap[index]);
        int bit = 0;
        while bit < 4 {
            if (nibble & (1 << bit)) != 0 {
                positions.push(offset + bit);
            }
            bit += 1;
        }
        offset += 4;
        index -= 1;
    }
    return positions;
}

isolated function hexValue(string character) returns int|error {
    return character == "0" ? 0 : character == "1" ? 1 : character == "2" ? 2 : character == "3" ? 3 :
        character == "4" ? 4 : character == "5" ? 5 : character == "6" ? 6 : character == "7" ? 7 :
        character == "8" ? 8 : character == "9" ? 9 : character == "A" || character == "a" ? 10 :
        character == "B" || character == "b" ? 11 : character == "C" || character == "c" ? 12 :
        character == "D" || character == "d" ? 13 : character == "E" || character == "e" ? 14 :
        character == "F" || character == "f" ? 15 : error("invalid CDC bitmap hexadecimal digit");
}

isolated function addChangedValue(map<anydata> changedData, Payload payload, string path) returns error? {
    string[] segments = re `\.`.split(path);
    if segments.length() == 1 {
        anydata? value = payload[segments[0]];
        changedData[segments[0]] = value;
        return;
    }
    if segments.length() != 2 {
        return error("CDC field path has unsupported nesting depth");
    }
    anydata? parentValue = payload[segments[0]];
    if parentValue !is map<anydata> {
        return error("CDC compound field value is missing or invalid");
    }
    map<anydata> nested = changedData[segments[0]] is map<anydata> ? <map<anydata>>changedData[segments[0]] : {};
    nested[segments[1]] = parentValue[segments[1]];
    changedData[segments[0]] = nested;
}
