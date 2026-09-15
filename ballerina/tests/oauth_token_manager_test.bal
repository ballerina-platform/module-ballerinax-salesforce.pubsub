import ballerina/http;
import ballerina/test;
import ballerina/time;
import ballerinax/salesforce;

final int OAUTH_FIXTURE_PORT = 19092;
final string OAUTH_FIXTURE_URL = "http://localhost:19092/token";
isolated int tokenFixtureRequests = 0;

listener http:Listener oauthFixture = check new (OAUTH_FIXTURE_PORT);

service /token on oauthFixture {
    resource function post .(@http:Payload string requestBody) returns json {
        lock {
            tokenFixtureRequests += 1;
        }
        return {access_token: "rotated-access-token", refresh_token: "rotated-refresh-token", expires_in: 3600};
    }
}

@test:Config {}
function testBearerTokenManagerReturnsConfiguredToken() returns error? {
    ConnectionConfig connection = {
        auth: <http:BearerTokenConfig>{token: "test-access-token"},
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };

    PubSubTokenManager manager = new (connection);
    test:assertEquals(check manager.getAccessToken(), "test-access-token");
}

@test:Config {}
function testRefreshTokenManagerPersistsRotatedTokenAndReusesIt() returns error? {
    int requestsBefore;
    lock {
        requestsBefore = tokenFixtureRequests;
    }
    salesforce:TokenStore store = new salesforce:InMemoryTokenStore();
    ConnectionConfig connection = {
        auth: <http:OAuth2RefreshTokenGrantConfig>{
            refreshUrl: OAUTH_FIXTURE_URL,
            refreshToken: "seed-refresh-token",
            clientId: "refresh-client",
            clientSecret: "refresh-secret"
        },
        tokenStore: store,
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };

    PubSubTokenManager first = new (connection);
    test:assertEquals(check first.getAccessToken(), "rotated-access-token");
    salesforce:TokenData? stored = check store.getTokenData("salesforce.pubsub:refresh-client");
    test:assertTrue(stored is salesforce:TokenData);
    if stored is salesforce:TokenData {
        test:assertEquals(stored.refreshToken, "rotated-refresh-token");
    }

    PubSubTokenManager second = new (connection);
    test:assertEquals(check second.getAccessToken(), "rotated-access-token");
    lock {
        test:assertEquals(tokenFixtureRequests, requestsBefore + 1);
    }
}

@test:Config {}
function testRefreshTokenManagerReplacesExpiredStoredToken() returns error? {
    salesforce:TokenStore store = new salesforce:InMemoryTokenStore();
    [int, decimal] now = time:utcNow();
    check store.setTokenData("salesforce.pubsub:expired-refresh-client", {
        accessToken: "expired-access-token",
        refreshToken: "seed-refresh-token",
        accessTokenExpiryEpoch: now[0] - 1,
        issuedAtEpoch: now[0] - 3600,
        lastRefreshedAtEpoch: now[0] - 3600
    });
    ConnectionConfig connection = {
        auth: <http:OAuth2RefreshTokenGrantConfig>{
            refreshUrl: OAUTH_FIXTURE_URL,
            refreshToken: "obsolete-seed-token",
            clientId: "expired-refresh-client",
            clientSecret: "refresh-secret"
        },
        tokenStore: store,
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };

    PubSubTokenManager manager = new (connection);
    test:assertEquals(check manager.getAccessToken(), "rotated-access-token");
    salesforce:TokenData? updated = check store.getTokenData("salesforce.pubsub:expired-refresh-client");
    test:assertTrue(updated is salesforce:TokenData);
    if updated is salesforce:TokenData {
        test:assertEquals(updated.refreshToken, "rotated-refresh-token");
    }
}

@test:Config {}
function testClientCredentialsTokenManagerCachesToken() returns error? {
    ConnectionConfig connection = {
        auth: {
            tokenUrl: OAUTH_FIXTURE_URL,
            clientId: "client-credentials-client",
            clientSecret: "client-credentials-secret"
        },
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };

    PubSubTokenManager manager = new (connection);
    test:assertEquals(check manager.getAccessToken(), "rotated-access-token");
    test:assertEquals(check manager.getAccessToken(), "rotated-access-token");
}

@test:Config {}
function testPasswordGrantTokenManagerCachesToken() returns error? {
    ConnectionConfig connection = {
        auth: {
            tokenUrl: OAUTH_FIXTURE_URL,
            username: "integration-user",
            password: "integration-password",
            clientId: "password-client",
            clientSecret: "password-secret"
        },
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };

    PubSubTokenManager manager = new (connection);
    test:assertEquals(check manager.getAccessToken(), "rotated-access-token");
    test:assertEquals(check manager.getAccessToken(), "rotated-access-token");
}
