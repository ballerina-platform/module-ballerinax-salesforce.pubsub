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

# Loads a schema document when it is absent from the process-wide cache.
public type SchemaLoader distinct object {
    #
    # + tenantId - Salesforce tenant/org that owns the schema
    # + schemaId - Salesforce writer schema ID
    # + return - Avro schema JSON or an error
    public isolated function load(string tenantId, string schemaId) returns string|error;
};

# Process-wide schema cache. It retains successful entries for the life of the
# process and does not retain failed loads.
public isolated class SchemaCache {
    private final map<string> schemas = {};

    # Returns a cached schema or loads it once while holding the per-cache
    # synchronization boundary. The lock makes concurrent misses for the same
    # entry single-flight in V1.
    #
    # + tenantId - Salesforce tenant/org that owns the schema
    # + schemaId - Salesforce writer schema ID
    # + loader - source used only for a cache miss
    # + return - Avro schema JSON or a load error
    public isolated function getOrLoad(string tenantId, string schemaId, SchemaLoader loader) returns string|error {
        string key = schemaCacheKey(tenantId, schemaId);
        lock {
            string? cached = self.schemas[key];
            if cached is string {
                return cached;
            }
            string schemaJson = check loader.load(tenantId, schemaId);
            self.schemas[key] = schemaJson;
            return schemaJson;
        }
    }
}

// A single cache is deliberately owned by the connector module so all
// Publishers and Listeners reuse writer schemas for the same Salesforce org.
final SchemaCache PROCESS_SCHEMA_CACHE = new;

// Resolves a schema through the connector-wide cache. This remains internal so
// applications cannot couple their own cache lifetime to connector behavior.
isolated function processSchemaFor(string tenantId, string schemaId, SchemaLoader loader) returns string|error {
    return PROCESS_SCHEMA_CACHE.getOrLoad(tenantId, schemaId, loader);
}

isolated function schemaCacheKey(string tenantId, string schemaId) returns string {
    return tenantId + "\u{001F}" + schemaId;
}
