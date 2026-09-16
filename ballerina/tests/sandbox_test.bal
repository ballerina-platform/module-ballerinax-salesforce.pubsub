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
import ballerina/lang.runtime;
import ballerina/os;
import ballerina/test;
import ballerina/uuid;
import ballerinax/salesforce;

// Match the existing Salesforce connector's CI convention: reusable workflows
// expose protected org secrets directly as environment variables.
public configurable string sandboxEndpoint = os:getEnv("EP_URL");
public configurable string sandboxAccessToken = os:getEnv("ACCESS_TOKEN");
public configurable string sandboxClientId = os:getEnv("CLIENT_ID");
public configurable string sandboxClientSecret = os:getEnv("CLIENT_SECRET");
public configurable string sandboxRefreshToken = os:getEnv("REFRESH_TOKEN");
public configurable string sandboxRefreshUrl = os:getEnv("REFRESH_URL");
public configurable string sandboxUsername = os:getEnv("SF_USERNAME");
public configurable string sandboxPassword = os:getEnv("SF_PASSWORD");
public configurable string sandboxTenantId = tenantIdFromSessionToken(sandboxAccessToken);
public configurable boolean pubsubSandboxTests = sandboxEndpoint != "" && sandboxAccessToken != "" &&
        sandboxClientId != "" && sandboxClientSecret != "" && sandboxRefreshUrl != "";

function tenantIdFromSessionToken(string sessionToken) returns string {
    int? separator = sessionToken.indexOf("!");
    return separator is int ? sessionToken.substring(0, separator) : "";
}

final string SANDBOX_EVENT_TOPIC = "/event/Order_Notification__e";
final string SANDBOX_CDC_TOPIC = "/data/ChangeEvents";
final int SANDBOX_CDC_WAIT_SECONDS = 20;
isolated string expectedSandboxAccountName = "";
isolated boolean sandboxCdcReceived = false;

isolated function setSandboxExpectedAccountName(string value) {
    lock { expectedSandboxAccountName = value; }
}

isolated function expectedSandboxName() returns string {
    lock { return expectedSandboxAccountName; }
}

isolated function resetSandboxCdcReceived() {
    lock { sandboxCdcReceived = false; }
}

isolated function markSandboxCdcReceived() {
    lock { sandboxCdcReceived = true; }
}

isolated function didReceiveSandboxCdc() returns boolean {
    lock { return sandboxCdcReceived; }
}

function sandboxBearerConnection() returns ConnectionConfig => {
    auth: <http:BearerTokenConfig>{token: sandboxAccessToken},
    instanceUrl: sandboxEndpoint,
    tenantId: sandboxTenantId
};

function sandboxClientCredentialsConnection() returns ConnectionConfig => {
    auth: {tokenUrl: sandboxRefreshUrl, clientId: sandboxClientId, clientSecret: sandboxClientSecret},
    instanceUrl: sandboxEndpoint,
    tenantId: sandboxTenantId
};

function sandboxPasswordConnection() returns ConnectionConfig => {
    auth: {
        tokenUrl: sandboxRefreshUrl,
        username: sandboxUsername,
        password: sandboxPassword,
        clientId: sandboxClientId,
        clientSecret: sandboxClientSecret
    },
    instanceUrl: sandboxEndpoint,
    tenantId: sandboxTenantId
};

@test:Config {groups: ["sandbox"], enable: pubsubSandboxTests}
function testSandboxClientCredentialsCanResolvePublishTopic() returns error? {
    Publisher publisher = check new ({
        connection: sandboxClientCredentialsConnection(),
        topic: SANDBOX_EVENT_TOPIC
    });
    _ = check publisher->getTopic();
}

@test:Config {groups: ["sandbox"], enable: pubsubSandboxTests}
function testSandboxPublishPlatformEvent() returns error? {
    string runId = "pubsub-sandbox-" + uuid:createType4AsString();
    Publisher publisher = check new ({connection: sandboxBearerConnection(), topic: SANDBOX_EVENT_TOPIC});
    PublishResult[] results = check publisher->publish([{
        id: runId,
        payload: {"CreatedDate": 0, "CreatedById": "", "Order_Id__c": runId, "Status__c": "TEST"}
    }]);
    test:assertEquals(results.length(), 1);
    test:assertEquals(results[0].id, runId);
    test:assertEquals(results[0].itemError, ());
}

// Account CDC delivery is intentionally a separate opt-in test. It creates one
// uniquely named Account, stops the Listener before deletion, and deletes only
// that returned Salesforce record ID. The bounded wait avoids retaining an idle
// generated Subscribe stream in a shared sandbox.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxAccountCdcLifecycle() returns error? {
    salesforce:Client rest = check new ({
        baseUrl: sandboxEndpoint,
        auth: <http:BearerTokenConfig>{token: sandboxAccessToken}
    });
    string runId = "pubsub-cdc-" + uuid:createType4AsString();
    setSandboxExpectedAccountName(runId);
    resetSandboxCdcReceived();
    Listener cdcListener = check new ({
        connection: sandboxBearerConnection(),
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            anydata? changedData = event.payload["changedData"];
            if changedData is map<anydata> && changedData["Name"] is string {
                if changedData["Name"] == expectedSandboxName() {
                    markSandboxCdcReceived();
                }
            }
        }
    };
    check cdcListener.attach(handler, SANDBOX_CDC_TOPIC);
    check cdcListener.'start();
    runtime:registerListener(cdcListener);
    salesforce:CreationResponse created = check rest->create("Account", {"Name": runId});
    int elapsed = 0;
    boolean received = false;
    while elapsed < SANDBOX_CDC_WAIT_SECONDS {
        received = didReceiveSandboxCdc();
        if received {
            break;
        }
        runtime:sleep(1);
        elapsed += 1;
    }
    // Stop before deleting: the cleanup delete must never satisfy this test.
    error? stopError = cdcListener.immediateStop();
    error? cleanup = rest->delete("Account", created.id);
    if stopError is error {
        return error("failed to stop sandbox CDC listener");
    }
    if cleanup is error {
        return error("failed to clean up sandbox Account");
    }
    test:assertTrue(received, "did not receive the Account CDC create event before timeout");
}
