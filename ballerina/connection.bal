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
import ballerina/http;
import ballerina/grpc;

final string DEFAULT_PUBSUB_ENDPOINT = "https://api.pubsub.salesforce.com:7443";

# Connection settings shared by Publishers and Listeners. Authentication uses
# the existing Salesforce connector's public OAuth2 configuration union so the
# supported grant types have one application-facing shape.
public type ConnectionConfig record {| 
    # OAuth configuration shared with `ballerinax/salesforce`.
    salesforce:OAuth2Config auth;
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
    if accessToken.length() == 0 {
        return error("access token must not be empty");
    }
    return {
        accesstoken: accessToken,
        instanceurl: config.instanceUrl,
        tenantid: config.tenantId
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
}

// Produces the transport configuration used for every owned gRPC channel.
// Keeping this mapping in one place prevents Publisher and Listener from
// diverging on TLS, proxy/pool, or timeout behavior.
isolated function grpcConfigFor(ConnectionConfig config) returns grpc:ClientConfiguration {
    grpc:ClientConfiguration transportConfig = config.grpcConfig.clone();
    transportConfig.timeout = config.connectionTimeout;
    return transportConfig;
}

# Returns a current access token for a connection. Bearer tokens are immediately
# usable; renewable grants require the Salesforce token-provider seam.
#
# + config - connection configuration
# + return - access token or an error for a grant awaiting shared lifecycle support
isolated function accessTokenFor(ConnectionConfig config) returns string|error {
    salesforce:OAuth2Config auth = config.auth;
    if auth is http:BearerTokenConfig {
        if auth.token.length() == 0 {
            return error("bearer token must not be empty");
        }
        return auth.token;
    }
    return error("renewable OAuth grants require the Salesforce OAuth2TokenProvider");
}
