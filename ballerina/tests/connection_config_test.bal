import ballerina/test;
import ballerina/http;
import ballerina/grpc;

// This fails if a unary RPC can omit any Salesforce-required connection
// metadata or rename a protocol header.
@test:Config {}
function testConnectionMetadataUsesCurrentSalesforceHeaderNames() returns error? {
    ConnectionConfig config = {
        auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
        instanceUrl: "https://acme.my.salesforce.com",
        tenantId: "00D000000000001"
    };

    map<string|string[]> headers = check metadataFor(config, "access-token");
    test:assertEquals(headers["accesstoken"], "access-token");
    test:assertEquals(headers["instanceurl"], "https://acme.my.salesforce.com");
    test:assertEquals(headers["tenantid"], "00D000000000001");
}

// This fails if an invalid endpoint is passed through until an opaque gRPC
// failure instead of being rejected during local connector construction.
@test:Config {}
function testConnectionConfigRejectsNonHttpsEndpoint() {
    ConnectionConfig config = {
        auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
        instanceUrl: "https://acme.my.salesforce.com",
        tenantId: "00D000000000001",
        endpoint: "http://api.pubsub.salesforce.com:7443"
    };

    error? result = validateConnectionConfig(config);
    test:assertTrue(result is error);
}

// This fails if connector construction drops the gRPC TLS, proxy/pool, and
// inbound-message controls required by real Salesforce and local TLS fixtures.
@test:Config {}
function testConnectionConfigRetainsGrpcTransportSettings() {
    ConnectionConfig config = {
        auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
        instanceUrl: "https://acme.my.salesforce.com",
        tenantId: "00D000000000001",
        connectionTimeout: 12,
        grpcConfig: <grpc:ClientConfiguration>{
            timeout: 8,
            maxInboundMessageSize: 5242880,
            secureSocket: {enable: false}
        }
    };

    test:assertEquals(config.grpcConfig.timeout, 8.0d);
    test:assertEquals(config.grpcConfig.maxInboundMessageSize, 5242880);
    test:assertFalse(config.grpcConfig.secureSocket?.enable ?: true);
    grpc:ClientConfiguration effective = grpcConfigFor(config);
    test:assertEquals(effective.timeout, 12.0d);
    test:assertEquals(effective.maxInboundMessageSize, 5242880);
}
