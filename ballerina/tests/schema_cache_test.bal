import ballerina/test;

isolated class CountingSchemaLoader {
    *SchemaLoader;
    private int calls = 0;

    public isolated function load(string tenantId, string schemaId) returns string|error {
        lock {
            self.calls += 1;
        }
        return "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    }

    isolated function callCount() returns int {
        lock {
            return self.calls;
        }
    }
}

isolated class FailingThenSuccessfulSchemaLoader {
    *SchemaLoader;
    private int calls = 0;

    public isolated function load(string tenantId, string schemaId) returns string|error {
        lock {
            self.calls += 1;
            if self.calls == 1 {
                return error("temporary schema lookup failure");
            }
        }
        return "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    }

    isolated function callCount() returns int {
        lock {
            return self.calls;
        }
    }
}

// This fails if a publisher and listener that request the same tenant/schema
// entry trigger duplicate GetSchema work rather than sharing one cache entry.
@test:Config {}
function testSchemaCacheReusesAResolvedTenantSchema() returns error? {
    SchemaCache cache = new;
    CountingSchemaLoader loader = new;

    string first = check cache.getOrLoad("00D000000000001", "schema-1", loader);
    string second = check cache.getOrLoad("00D000000000001", "schema-1", loader);

    test:assertEquals(first, "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}");
    test:assertEquals(second, first);
    test:assertEquals(loader.callCount(), 1);
}

// This fails if schema IDs are cached globally without tenant scoping, which
// could decode an event using a schema fetched for another Salesforce org.
@test:Config {}
function testSchemaCacheScopesEntriesByTenant() returns error? {
    SchemaCache cache = new;
    CountingSchemaLoader loader = new;

    _ = check cache.getOrLoad("00D000000000001", "schema-1", loader);
    _ = check cache.getOrLoad("00D000000000002", "schema-1", loader);

    test:assertEquals(loader.callCount(), 2);
}

// This fails if a transient GetSchema failure is retained and prevents a later
// successful lookup, contrary to the V1 cache decision.
@test:Config {}
function testSchemaCacheDoesNotCacheFailedLoads() returns error? {
    SchemaCache cache = new;
    FailingThenSuccessfulSchemaLoader loader = new;

    string|error first = cache.getOrLoad("00D000000000001", "schema-1", loader);
    test:assertTrue(first is error);
    string second = check cache.getOrLoad("00D000000000001", "schema-1", loader);

    test:assertEquals(second, "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}");
    test:assertEquals(loader.callCount(), 2);
}

// The connector cache must be shared by Publisher and Listener instances. This
// package-level seam exercises that shared cache without exposing it publicly.
@test:Config {}
function testProcessSchemaCacheIsSharedAcrossCallers() returns error? {
    CountingSchemaLoader loader = new;

    string first = check processSchemaFor("00D000000000003", "shared-schema-1", loader);
    string second = check processSchemaFor("00D000000000003", "shared-schema-1", loader);

    test:assertEquals(first, "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}");
    test:assertEquals(second, first);
    test:assertEquals(loader.callCount(), 1);
}
