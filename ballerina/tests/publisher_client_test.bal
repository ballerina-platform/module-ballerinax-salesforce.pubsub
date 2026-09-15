import ballerina/http;
import ballerina/test;

// Invalid publisher settings must fail before any remote call starts.
@test:Config {}
function testPublisherRejectsInvalidConstructionConfig() {
    PublisherConfig config = {
        connection: {
            auth: <http:BearerTokenConfig>{token: "test-token"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        },
        topic: ""
    };

    Publisher|error publisher = new (config);
    test:assertTrue(publisher is error);
}
