import ballerina/test;
import ballerina/http;

// This fails if the connector's default request-splitting target drifts from
// the agreed safe 3 MB target below Salesforce's 4 MB hard limit.
@test:Config {}
function testPublisherConfigUsesV1BatchDefaults() {
    PublisherConfig config = {
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        },
        topic: "/event/Order__e"
    };

    test:assertEquals(config.targetRequestSizeBytes, 3 * 1024 * 1024);
}
