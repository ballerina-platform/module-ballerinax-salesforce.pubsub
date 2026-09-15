import ballerina/http;
import ballerina/test;

// This fails if the public bearer-token OAuth configuration cannot be used for
// an authenticated Pub/Sub RPC while the shared provider seam is unreleased.
@test:Config {}
function testBearerConnectionProvidesAccessToken() returns error? {
    ConnectionConfig config = {
        auth: <http:BearerTokenConfig>{token: "test-token"},
        instanceUrl: "https://acme.my.salesforce.com",
        tenantId: "00D000000000001"
    };

    test:assertEquals(check accessTokenFor(config), "test-token");
}
