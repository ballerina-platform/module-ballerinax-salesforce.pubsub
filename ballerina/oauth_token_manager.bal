// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.

import ballerina/http;
import ballerina/oauth2;
import ballerina/time;
import ballerina/url;
import ballerinax/salesforce;

const int TOKEN_REFRESH_BUFFER_SECONDS = 60;
const int TOKEN_STORE_LOCK_TTL_SECONDS = 30;

# Connector-internal OAuth token lifecycle. It deliberately uses the public
# Salesforce OAuth configuration and TokenStore contracts without depending on
# the Salesforce connector's private CometD token manager.
isolated class PubSubTokenManager {
    private final readonly & salesforce:OAuth2Config auth;
    private final salesforce:TokenStore tokenStore;
    private final int sessionTimeout;
    private oauth2:ClientOAuth2Provider? grantProvider = ();

    isolated function init(ConnectionConfig connection) {
        self.auth = connection.auth.cloneReadOnly();
        self.tokenStore = connection.tokenStore;
        self.sessionTimeout = connection.sessionTimeout;
    }

    # Returns a current OAuth token. Refresh-token grants keep their rotated
    # token in the supplied TokenStore; the other standard OAuth grants use
    # Ballerina's process-local OAuth provider cache.
    isolated function getAccessToken() returns string|error {
        readonly & salesforce:OAuth2Config auth = self.auth;
        if auth is http:BearerTokenConfig {
            if auth.token.length() == 0 {
                return error("bearer token must not be empty");
            }
            return auth.token;
        }
        if auth is http:OAuth2RefreshTokenGrantConfig {
            return self.refreshTokenGrant(auth);
        }
        return self.standardGrantToken(auth);
    }

    # Invalidates only the cached access token after an authentication failure.
    # A refresh token is deliberately retained so the next request can renew it.
    isolated function invalidateAccessToken() returns error? {
        salesforce:OAuth2Config auth = self.auth;
        if auth is http:OAuth2RefreshTokenGrantConfig {
            string storeKey = "salesforce.pubsub:" + auth.clientId;
            salesforce:TokenData? tokenData = check self.tokenStore.getTokenData(storeKey);
            if tokenData is salesforce:TokenData {
                tokenData.accessTokenExpiryEpoch = 0;
                check self.tokenStore.setTokenData(storeKey, tokenData);
            }
            return;
        }
        lock {
            self.grantProvider = ();
        }
    }

    // Holding the lock across the whole check-then-create sequence (not just
    // the field write) makes concurrent callers on a cold cache single-flight:
    // a second caller blocks until the first has published a provider, then
    // reuses it instead of racing to construct its own.
    private isolated function standardGrantToken(readonly & salesforce:OAuth2Config auth) returns string|error {
        lock {
            oauth2:ClientOAuth2Provider? existingProvider = self.grantProvider;
            oauth2:ClientOAuth2Provider provider;
            if existingProvider is oauth2:ClientOAuth2Provider {
                provider = existingProvider;
            } else {
                oauth2:ClientCredentialsGrantConfig|oauth2:PasswordGrantConfig grant =
                        check auth.cloneWithType();
                provider = new (grant);
                self.grantProvider = provider;
            }
            string|oauth2:Error token = provider.generateToken();
            return token is string ? token : error("OAuth token request failed");
        }
    }

    // As above: the whole refresh-token critical section runs inside one lock
    // so concurrent local callers serialize onto a single HTTP refresh instead
    // of each independently missing the store's cache. The store's own
    // acquireLock/releaseLock remain for a real distributed TokenStore shared
    // across processes, where this process-local lock offers no protection.
    private isolated function refreshTokenGrant(readonly & http:OAuth2RefreshTokenGrantConfig auth) returns string|error {
        string storeKey = "salesforce.pubsub:" + auth.clientId;
        lock {
            salesforce:TokenData? cached = check self.tokenStore.getTokenData(storeKey);
            if cached is salesforce:TokenData && isUsable(cached) {
                return cached.accessToken;
            }
            boolean acquired = check self.tokenStore.acquireLock(storeKey, TOKEN_STORE_LOCK_TTL_SECONDS);
            if !acquired {
                return error("OAuth token refresh is in progress in another process");
            }
            string|error result = refreshWhileLocked(self.tokenStore, auth, storeKey, self.sessionTimeout);
            error? releaseError = self.tokenStore.releaseLock(storeKey);
            if result is error {
                return result;
            }
            if releaseError is error {
                return releaseError;
            }
            return result;
        }
    }
}

isolated function refreshWhileLocked(salesforce:TokenStore tokenStore, readonly & http:OAuth2RefreshTokenGrantConfig auth,
        string storeKey, int sessionTimeout) returns string|error {
    salesforce:TokenData? cached = check tokenStore.getTokenData(storeKey);
    if cached is salesforce:TokenData && isUsable(cached) {
        return cached.accessToken;
    }
    string refreshToken = cached is salesforce:TokenData && cached.refreshToken.length() > 0 ?
            cached.refreshToken : auth.refreshToken;
    http:Client tokenClient = check new (auth.refreshUrl);
    string encodedRefresh = check url:encode(refreshToken, "UTF-8");
    string encodedClientId = check url:encode(auth.clientId, "UTF-8");
    string encodedClientSecret = check url:encode(auth.clientSecret, "UTF-8");
    string body = "grant_type=refresh_token&refresh_token=" + encodedRefresh + "&client_id=" + encodedClientId +
            "&client_secret=" + encodedClientSecret;
    http:Response response = check tokenClient->post("", body, mediaType = "application/x-www-form-urlencoded");
    json responseBody = check response.getJsonPayload();
    if response.statusCode < 200 || response.statusCode >= 300 {
        if responseBody is map<json> && responseBody.hasKey("error") && responseBody["error"] == "invalid_grant" {
            check tokenStore.clearTokenData(storeKey);
            return error("OAuth refresh token is no longer valid; re-authentication is required");
        }
        return error("OAuth token request failed");
    }
    string accessToken = check (check responseBody.access_token).ensureType(string);
    string rotatedRefreshToken = refreshToken;
    json|error rotated = responseBody.refresh_token;
    if rotated is string && rotated.length() > 0 {
        rotatedRefreshToken = rotated;
    }
    [int, decimal] now = time:utcNow();
    int lifetime = sessionTimeout;
    json|error expiresIn = responseBody.expires_in;
    if expiresIn is int && expiresIn > TOKEN_REFRESH_BUFFER_SECONDS {
        lifetime = expiresIn;
    }
    int expiresAt = now[0] + lifetime - TOKEN_REFRESH_BUFFER_SECONDS;
    check tokenStore.setTokenData(storeKey, {
        accessToken,
        refreshToken: rotatedRefreshToken,
        accessTokenExpiryEpoch: expiresAt,
        issuedAtEpoch: now[0],
        lastRefreshedAtEpoch: now[0]
    });
    return accessToken;
}

isolated function isUsable(salesforce:TokenData tokenData) returns boolean {
    [int, decimal] now = time:utcNow();
    return tokenData.accessToken.length() > 0 && tokenData.accessTokenExpiryEpoch > now[0];
}
