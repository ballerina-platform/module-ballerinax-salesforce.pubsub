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

import ballerina/http;
import ballerina/test;
import ballerina/time;
import ballerinax/salesforce;

final int OAUTH_FIXTURE_PORT = 19092;
final string OAUTH_FIXTURE_URL = "http://localhost:19092/token";
final string OAUTH_FIXTURE_NON_JSON_URL = "http://localhost:19092/non-json-error";
final string OAUTH_FIXTURE_NON_JSON_SUCCESS_URL = "http://localhost:19092/non-json-success";
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

// Reproduces a misconfigured/unreachable token endpoint: a non-2xx response
// whose body is not JSON at all (an HTML error page, a plain-text 404, a
// proxy/WAF block page) rather than Salesforce's own documented
// `{"error": "..."}` shape.
service /non\-json\-error on oauthFixture {
    resource function post .() returns http:NotFound {
        return {body: "<html><body>Not Found</body></html>", mediaType: "text/html"};
    }
}

// Reproduces a misconfigured refreshUrl that resolves to something other than
// the token endpoint but still answers with a 2xx (a login page, a captive
// portal, a redirect target) -- the status check alone can't catch this, only
// the JSON parse can.
service /non\-json\-success on oauthFixture {
    resource function post .() returns http:Response {
        http:Response response = new;
        response.setTextPayload("<html><body>login page</body></html>", "text/html");
        return response;
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

// A cold cache with several concurrent callers on one manager must not send
// one refresh request per caller; the TokenStore lock should serialize them
// onto a single refresh, with every caller still receiving a usable token.
@test:Config {}
function testConcurrentRefreshTokenRequestsShareOneRefresh() returns error? {
    int requestsBefore;
    lock {
        requestsBefore = tokenFixtureRequests;
    }
    ConnectionConfig connection = {
        auth: <http:OAuth2RefreshTokenGrantConfig>{
            refreshUrl: OAUTH_FIXTURE_URL,
            refreshToken: "seed-refresh-token",
            clientId: "concurrent-refresh-client",
            clientSecret: "refresh-secret"
        },
        tokenStore: new salesforce:InMemoryTokenStore(),
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };
    PubSubTokenManager manager = new (connection);

    future<string|error>[] calls = [];
    foreach int _ in 0 ..< 5 {
        future<string|error> call = start manager.getAccessToken();
        calls.push(call);
    }
    string[] tokens = [];
    foreach future<string|error> call in calls {
        string|error result = wait call;
        if result is string {
            tokens.push(result);
        }
    }
    test:assertEquals(tokens.length(), 5, "every concurrent caller should receive a usable token");
    foreach string token in tokens {
        test:assertEquals(token, "rotated-access-token");
    }
    lock {
        test:assertEquals(tokenFixtureRequests, requestsBefore + 1,
            "concurrent callers on a cold cache must share a single refresh request");
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

// This fails if invalidating a stored access token panics instead of
// returning a normal error: InMemoryTokenStore.getTokenData() can return a
// `readonly` record, and mutating a field on that value in place (rather than
// building a new record) throws an uncatchable Ballerina inherent-type-violation
// panic -- observed live via a real Listener whose first GetTopic call got
// UNAUTHENTICATED, which crashed the whole worker strand silently instead of
// refreshing the token and retrying.
@test:Config {}
function testInvalidateAccessTokenClearsStoredExpiryWithoutPanicking() returns error? {
    salesforce:TokenStore store = new salesforce:InMemoryTokenStore();
    [int, decimal] now = time:utcNow();
    check store.setTokenData("salesforce.pubsub:invalidate-client", {
        accessToken: "still-valid-access-token",
        refreshToken: "seed-refresh-token",
        accessTokenExpiryEpoch: now[0] + 3600,
        issuedAtEpoch: now[0],
        lastRefreshedAtEpoch: now[0]
    });
    ConnectionConfig connection = {
        auth: <http:OAuth2RefreshTokenGrantConfig>{
            refreshUrl: OAUTH_FIXTURE_URL,
            refreshToken: "seed-refresh-token",
            clientId: "invalidate-client",
            clientSecret: "refresh-secret"
        },
        tokenStore: store,
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };

    PubSubTokenManager manager = new (connection);
    check manager.invalidateAccessToken();

    salesforce:TokenData? invalidated = check store.getTokenData("salesforce.pubsub:invalidate-client");
    test:assertTrue(invalidated is salesforce:TokenData);
    if invalidated is salesforce:TokenData {
        test:assertEquals(invalidated.accessTokenExpiryEpoch, 0);
        test:assertEquals(invalidated.accessToken, "still-valid-access-token",
            "invalidation clears only the expiry, not the access token itself");
    }
}

// This fails if a non-2xx token-endpoint response whose body isn't JSON at
// all (an HTML error page, a wrong-endpoint 404, a proxy/WAF block page)
// surfaces as Ballerina's generic "error occurred while retrieving the json
// payload" instead of a clear, status-code-including OAuth failure.
@test:Config {}
function testRefreshTokenManagerReportsStatusOnNonJsonErrorResponse() returns error? {
    ConnectionConfig connection = {
        auth: <http:OAuth2RefreshTokenGrantConfig>{
            refreshUrl: OAUTH_FIXTURE_NON_JSON_URL,
            refreshToken: "seed-refresh-token",
            clientId: "non-json-error-client",
            clientSecret: "refresh-secret"
        },
        tokenStore: new salesforce:InMemoryTokenStore(),
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };

    PubSubTokenManager manager = new (connection);
    string|error result = manager.getAccessToken();
    test:assertTrue(result is error, "a non-JSON error response must not be treated as a usable token");
    if result is error {
        string message = result.message();
        test:assertTrue(message.includes("404"), "expected the real HTTP status to be in the error message, got: " + message);
    }
}

// This fails if a 2xx token-endpoint response whose body isn't JSON (a
// misconfigured refreshUrl that lands on a login page or redirect target
// instead of the real token endpoint) surfaces as Ballerina's generic "error
// occurred while retrieving the json payload" instead of a clear,
// status-code-including OAuth failure.
@test:Config {}
function testRefreshTokenManagerReportsStatusOnNonJsonSuccessResponse() returns error? {
    ConnectionConfig connection = {
        auth: <http:OAuth2RefreshTokenGrantConfig>{
            refreshUrl: OAUTH_FIXTURE_NON_JSON_SUCCESS_URL,
            refreshToken: "seed-refresh-token",
            clientId: "non-json-success-client",
            clientSecret: "refresh-secret"
        },
        tokenStore: new salesforce:InMemoryTokenStore(),
        instanceUrl: "https://example.my.salesforce.com",
        tenantId: "00Dtest"
    };

    PubSubTokenManager manager = new (connection);
    string|error result = manager.getAccessToken();
    test:assertTrue(result is error, "a non-JSON 2xx response must not be treated as a usable token");
    if result is error {
        string message = result.message();
        test:assertTrue(message.includes("200"), "expected the real HTTP status to be in the error message, got: " + message);
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
