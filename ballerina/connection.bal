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

import ballerinax/salesforce;
import ballerina/grpc;
import ballerina/http;

final string DEFAULT_PUBSUB_ENDPOINT = "https://api.pubsub.salesforce.com:7443";

type ConnectionIdentity record {|
    string instanceUrl;
    string tenantId;
|};

isolated function connectionIdentityFor(ConnectionConfig config) returns readonly & ConnectionIdentity =>
    <readonly & ConnectionIdentity>{instanceUrl: config.instanceUrl, tenantId: config.tenantId};

# Connection settings shared by Publishers and Listeners. Authentication uses
# the existing Salesforce connector's public OAuth2 configuration union so the
# supported grant types have one application-facing shape.
public type ConnectionConfig record {| 
    # OAuth configuration shared with `ballerinax/salesforce`.
    salesforce:OAuth2Config auth;
    # Token state used by renewable OAuth grants. The default is appropriate
    # for one process; callers can supply a shared Salesforce TokenStore when
    # rotated refresh tokens must survive restarts.
    salesforce:TokenStore tokenStore = new salesforce:InMemoryTokenStore();
    # Fallback Salesforce session lifetime in seconds when the token endpoint
    # does not return an `expires_in` value.
    int sessionTimeout = 900;
    # Salesforce instance URL included in Pub/Sub RPC metadata.
    string instanceUrl;
    # Salesforce tenant/org identifier included in Pub/Sub RPC metadata.
    string tenantId;
    # Salesforce Pub/Sub gRPC endpoint.
    string endpoint = DEFAULT_PUBSUB_ENDPOINT;
    # Maximum time to establish an RPC connection, in seconds.
    decimal connectionTimeout = 30;
    # Advanced gRPC transport controls for TLS trust, proxy/pooling, request
    # timeout, compression, and inbound message limits. `connectionTimeout`
    # takes precedence over this record's `timeout` field.
    grpc:ClientConfiguration grpcConfig = {};
|};

# Builds the three metadata headers required by Salesforce Pub/Sub. The access
# token is supplied by the internal token lifecycle immediately before an RPC.
#
# + config - validated shared connection configuration
# + accessToken - current OAuth access token
# + return - Salesforce Pub/Sub RPC metadata
isolated function metadataFor(ConnectionConfig config, string accessToken) returns map<string|string[]>|error {
    check validateConnectionConfig(config);
    return metadataForIdentity({instanceUrl: config.instanceUrl, tenantId: config.tenantId}, accessToken);
}

isolated function metadataForIdentity(ConnectionIdentity connection, string accessToken)
        returns map<string|string[]>|error {
    if accessToken.length() == 0 {
        return error("access token must not be empty");
    }
    return {
        accesstoken: accessToken,
        instanceurl: connection.instanceUrl,
        tenantid: connection.tenantId
    };
}

# Validates configuration that can be rejected before a channel or RPC exists.
#
# + config - connection configuration to validate
# + return - an error when a value is invalid
isolated function validateConnectionConfig(ConnectionConfig config) returns error? {
    if !config.endpoint.startsWith("https://") {
        return error("endpoint must use https");
    }
    if !config.instanceUrl.startsWith("https://") {
        return error("instanceUrl must use https");
    }
    if config.tenantId.length() == 0 {
        return error("tenantId must not be empty");
    }
    if config.connectionTimeout <= 0.0d {
        return error("connectionTimeout must be greater than zero");
    }
    if config.sessionTimeout <= 60 {
        return error("sessionTimeout must be greater than 60 seconds");
    }
    salesforce:OAuth2Config auth = config.auth;
    if auth is http:OAuth2RefreshTokenGrantConfig && !isAllowedRefreshUrl(auth.refreshUrl) {
        return error("auth.refreshUrl must use https (plain http is only allowed against localhost/127.0.0.1, for local testing)");
    }
}

// A refresh URL is either https, or plain http against a loopback address --
// the latter exists only so local test fixtures (an in-process OAuth server)
// don't need a self-signed cert of their own. Any other plain-http host would
// send the refresh token and client secret over the network in cleartext.
isolated function isAllowedRefreshUrl(string refreshUrl) returns boolean {
    if refreshUrl.startsWith("https://") {
        return true;
    }
    return refreshUrl.startsWith("http://localhost:") || refreshUrl == "http://localhost" ||
        refreshUrl.startsWith("http://localhost/") ||
        refreshUrl.startsWith("http://127.0.0.1:") || refreshUrl == "http://127.0.0.1" ||
        refreshUrl.startsWith("http://127.0.0.1/");
}

// Produces the transport configuration used for one-shot RPCs (Publish,
// GetTopic, GetSchema outside a Listener's own channel). `connectionTimeout`
// is an appropriate call deadline for these: each completes quickly or fails.
isolated function grpcConfigFor(ConnectionConfig config) returns grpc:ClientConfiguration {
    grpc:ClientConfiguration transportConfig = config.grpcConfig.clone();
    transportConfig.timeout = config.connectionTimeout;
    return transportConfig;
}

// A Subscribe stream can sit idle for long stretches between events with no
// bytes flowing in either direction -- that is normal, healthy operation, not
// a stall. `grpc:ClientConfiguration.timeout` (60s default) closes the whole
// connection once that long since the last response, so applying
// `connectionTimeout` (or the module default) the same way `grpcConfigFor`
// does for one-shot RPCs would tear down a perfectly healthy long-lived
// stream on every quiet period; this effectively disables it for the
// Listener's channel instead, since liveness is judged by whether the stream
// is still open, not by a fixed deadline on how long it may stay idle.
final decimal STREAM_TIMEOUT_SECONDS = 31536000;

isolated function grpcConfigForListener(ConnectionConfig config) returns grpc:ClientConfiguration {
    grpc:ClientConfiguration transportConfig = config.grpcConfig.clone();
    transportConfig.timeout = STREAM_TIMEOUT_SECONDS;
    return transportConfig;
}

# Compatibility helper for package-internal callers. Publisher and Listener
# retain a manager for their full lifecycle; do not use this helper for a
# repeated RPC path because it intentionally creates a fresh manager.
isolated function accessTokenFor(ConnectionConfig config) returns string|error {
    PubSubTokenManager manager = new (config);
    return manager.getAccessToken();
}
