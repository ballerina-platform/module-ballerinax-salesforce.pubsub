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

import ballerina/avro;

# Encodes a dynamic connector payload using the exact Salesforce writer schema.
#
# + schemaJson - Avro schema returned by GetSchema
# + payload - dynamic connector payload
# + return - Avro-encoded event bytes
public isolated function encodePayload(string schemaJson, Payload payload) returns byte[]|error {
    avro:Schema schema = check new (schemaJson);
    Payload normalized = check normalizePayloadForAvro(schemaJson, payload);
    return schema.toAvro(normalized);
}

# Decodes event bytes using the exact writer schema named in the envelope.
#
# + schemaJson - Avro schema returned by GetSchema
# + payload - Avro event bytes
# + return - dynamic connector payload
public isolated function decodePayload(string schemaJson, byte[] payload) returns Payload|error {
    avro:Schema schema = check new (schemaJson);
    return schema.fromAvro(payload);
}
