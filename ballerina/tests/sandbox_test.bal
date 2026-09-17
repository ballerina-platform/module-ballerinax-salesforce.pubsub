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
import ballerina/log;
import ballerina/os;
import ballerina/test;
import ballerina/url;
import ballerina/uuid;
import ballerinax/salesforce;
import ballerinax/salesforce.pubsub.internal as pubsubApi;

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
// Deliberately excluded from the automated build: the shared reusable
// workflows forward every repo/org secret into the job environment (see the
// comment above), so the sandbox credentials above would otherwise be
// present and these tests would run against a real Salesforce sandbox on
// every CI build. GitHub Actions sets CI=true for every job by default; a
// developer's local shell does not, so `bal test --groups sandbox,sandbox-cdc`
// still runs them intentionally outside CI.
public configurable boolean pubsubSandboxTests = os:getEnv("CI") == "" && sandboxEndpoint != "" &&
        sandboxAccessToken != "" && sandboxClientId != "" && sandboxClientSecret != "" && sandboxRefreshUrl != "";

function tenantIdFromSessionToken(string sessionToken) returns string {
    int? separator = sessionToken.indexOf("!");
    return separator is int ? sessionToken.substring(0, separator) : "";
}

final string SANDBOX_EVENT_TOPIC = "/event/Order_Notification__e";
final string SANDBOX_CDC_TOPIC = "/data/ChangeEvents";
// Real CDC publish latency in a shared sandbox is variable and occasionally
// exceeds 20s with no transport error (confirmed via getLastError() staying
// () on the slow runs); 30s absorbs that variance without masking a genuine
// delivery failure, which would still show up as a populated ListenerError.
final int SANDBOX_CDC_WAIT_SECONDS = 30;

// Every Account this suite ever creates is named with this prefix. Both the
// before-suite sweep and the after-suite fallback use it to find and remove
// only artifacts this suite itself could have created, never unrelated
// sandbox data, so a crashed or forcibly cancelled prior run cannot leave
// permanent pollution behind.
final string SANDBOX_ACCOUNT_PREFIX = "pubsub-cdc-";

// Open record: Salesforce SOQL responses attach an "attributes" metadata
// field to every record that a closed record type would reject.
type SandboxAccountId record {
    string Id;
};
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

isolated int sandboxCdcEventsSeen = 0;

isolated function countSandboxCdcEvent() {
    lock { sandboxCdcEventsSeen += 1; }
}

isolated function sandboxCdcEventsSeenCount() returns int {
    lock { return sandboxCdcEventsSeen; }
}

function sandboxBearerConnection() returns ConnectionConfig => {
    auth: <http:BearerTokenConfig>{token: sandboxAccessToken},
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

function sandboxRestClient() returns salesforce:Client|error =>
    new ({baseUrl: sandboxEndpoint, auth: <http:BearerTokenConfig>{token: sandboxAccessToken}});

// Salesforce has no direct REST resource for undelete (confirmed empirically:
// composite/sobjects/undelete returns 404). The supported approach is the
// Tooling API's executeAnonymous running Database.undelete(); this requires
// the calling user to have "Author Apex", same as any anonymous Apex run.
function sandboxUndeleteAccount(string id) returns error? {
    http:Client toolingClient = check new (sandboxEndpoint, auth = <http:BearerTokenConfig>{token: sandboxAccessToken});
    string apex = string `Database.undelete(new List<Id>{Id.valueOf('${id}')});`;
    string encodedApex = check url:encode(apex, "UTF-8");
    json response = check toolingClient->get(
        string `/services/data/v59.0/tooling/executeAnonymous/?anonymousBody=${encodedApex}`);
    map<json> result = <map<json>>response;
    boolean success = result["success"] == true;
    if !success {
        return error("anonymous Apex undelete failed: " + response.toString());
    }
}

type CapturedCdcEvent record {|
    string changeType;
    string[] recordIds;
    map<anydata> changedData;
|};

isolated function toCapturedCdcEvent(Event event) returns CapturedCdcEvent {
    anydata? changedDataValue = event.payload["changedData"];
    anydata? metadataValue = event.payload["metadata"];
    map<anydata> changedData = changedDataValue is map<anydata> ? changedDataValue : {};
    map<anydata> metadata = metadataValue is map<anydata> ? metadataValue : {};
    string changeType = metadata["changeType"] is string ? <string>metadata["changeType"] : "";
    string[] recordIds = [];
    anydata? recordIdsValue = metadata["recordIds"];
    if recordIdsValue is anydata[] {
        foreach anydata id in recordIdsValue {
            if id is string {
                recordIds.push(id);
            }
        }
    }
    return {changeType, recordIds: recordIds.clone(), changedData: changedData.clone()};
}

// Records every observed CDC event's changeType/recordIds/changedData rather
// than a single boolean flag, so scenarios needing ordering, multi-record
// burst matching, or field-value assertions on DELETE/UNDELETE (which carry
// no "Name" in changedData) can all match by recordIds membership instead of
// relying on the Name field, which only CREATE/UPDATE populate.
isolated class SandboxCdcCollector {
    private CapturedCdcEvent[] captured = [];

    isolated function capture(Event event) {
        CapturedCdcEvent entry = toCapturedCdcEvent(event);
        lock {
            self.captured.push(entry.clone());
        }
    }

    isolated function snapshot() returns CapturedCdcEvent[] {
        lock {
            return self.captured.clone();
        }
    }
}

// Returns true exactly once, then false on every subsequent call -- used by
// the handler-failure-continuity scenario to fail only the first delivery
// attempt of one specific event, not its retry.
isolated class SandboxFailOnceGate {
    private boolean triggered = false;

    isolated function shouldFailOnce() returns boolean {
        lock {
            if !self.triggered {
                self.triggered = true;
                return true;
            }
            return false;
        }
    }
}

// Polls the collector's snapshot until an event matching (recordId,
// changeType) appears, or the timeout elapses. Returns the first match so
// callers can assert on its changedData.
function waitForChangeEvent(SandboxCdcCollector collector, string recordId, string changeType,
        int timeoutSeconds) returns CapturedCdcEvent? {
    int elapsed = 0;
    while elapsed < timeoutSeconds {
        foreach CapturedCdcEvent captured in collector.snapshot() {
            if captured.changeType == changeType && captured.recordIds.indexOf(recordId) is int {
                return captured;
            }
        }
        runtime:sleep(1);
        elapsed += 1;
    }
    return ();
}

// Polls until every recordId in the list has a matching-changeType event, or
// the timeout elapses.
function waitForAllChangeEvents(SandboxCdcCollector collector, string[] recordIds, string changeType,
        int timeoutSeconds) returns boolean {
    int elapsed = 0;
    while elapsed < timeoutSeconds {
        CapturedCdcEvent[] snapshot = collector.snapshot();
        boolean allFound = true;
        foreach string recordId in recordIds {
            boolean found = false;
            foreach CapturedCdcEvent captured in snapshot {
                if captured.changeType == changeType && captured.recordIds.indexOf(recordId) is int {
                    found = true;
                    break;
                }
            }
            if !found {
                allFound = false;
                break;
            }
        }
        if allFound {
            return true;
        }
        runtime:sleep(1);
        elapsed += 1;
    }
    return false;
}

// Every captured event (any changeType) whose recordIds include recordId, in
// arrival order -- used to assert delivery ordering across multiple changes
// to the same record.
function capturedEventsForRecord(SandboxCdcCollector collector, string recordId) returns CapturedCdcEvent[] =>
    from CapturedCdcEvent captured in collector.snapshot()
    where captured.recordIds.indexOf(recordId) is int
    select captured;

// Polls until at least minCount events (any changeType) for recordId have
// arrived, or the timeout elapses. Returns whatever arrived either way, so
// the caller gets a useful diagnostic snapshot on a timeout too.
function waitForAtLeastNEventsForRecord(SandboxCdcCollector collector, string recordId, int minCount,
        int timeoutSeconds) returns CapturedCdcEvent[] {
    int elapsed = 0;
    CapturedCdcEvent[] matches = capturedEventsForRecord(collector, recordId);
    while matches.length() < minCount && elapsed < timeoutSeconds {
        runtime:sleep(1);
        elapsed += 1;
        matches = capturedEventsForRecord(collector, recordId);
    }
    return matches;
}

// Shared Listener lifecycle for a scenario test: attach the given handler to
// topic, start, run the scenario body, then always stop before returning so a
// scenario's own assertion failure never leaks a running Subscribe stream. If
// the body fails, a populated ListenerError is folded into the returned error
// message -- this is what distinguishes "transport/listener actually died" as
// the failure cause from "no matching event arrived within the wait window".
function runSandboxCdcScenario(string topic, SubscriptionConfig subscriptionConfig, Service handler,
        function() returns error? body) returns error? {
    Listener cdcListener = check new ({connection: sandboxBearerConnection(), subscriptionConfig});
    check cdcListener.attach(handler, topic);
    check cdcListener.'start();
    runtime:registerListener(cdcListener);
    error? bodyResult = body();
    ListenerError? lastError = cdcListener.getLastError();
    error? stopResult = cdcListener.immediateStop();
    if bodyResult is error {
        if lastError is ListenerError {
            return error(bodyResult.message() + " [listener terminated: operation=" + lastError.operation +
                " grpcStatus=" + (lastError.grpcStatus ?: "()") + " cause=" + lastError.cause.message() + "]");
        }
        return bodyResult;
    }
    return stopResult;
}

// Deletes every Account whose Name carries this suite's marker prefix. Used
// both before the suite (to clear artifacts a previous run left behind after
// a crash or forced cancellation) and after it (as a fallback in case a test
// failed in a way that skipped its own inline cleanup). Never fails the
// sweep over a single record's delete failure; only the query itself, which
// signals a broader connectivity/credential problem, is propagated.
function sweepStaleSandboxAccounts() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    stream<SandboxAccountId, error?> matches =
        check rest->query(string `SELECT Id FROM Account WHERE Name LIKE '${SANDBOX_ACCOUNT_PREFIX}%'`);
    int swept = 0;
    check from SandboxAccountId account in matches
        do {
            error? deleted = rest->delete("Account", account.Id);
            if deleted is error {
                log:printWarn("sandbox sweep: failed to delete stale Account " + account.Id + ": " +
                    deleted.message());
            } else {
                swept += 1;
            }
        };
    if swept > 0 {
        log:printInfo("sandbox sweep: removed " + swept.toString() + " stale Account record(s)");
    }
}

@test:BeforeSuite
function sandboxBeforeSuite() returns error? {
    if !pubsubSandboxTests {
        return;
    }
    check sweepStaleSandboxAccounts();
}

@test:AfterSuite {alwaysRun: true}
function sandboxAfterSuite() returns error? {
    if !pubsubSandboxTests {
        return;
    }
    check sweepStaleSandboxAccounts();
}

@test:Config {groups: ["sandbox"], enable: pubsubSandboxTests}
function testSandboxBearerTokenCanResolvePublishTopic() returns error? {
    Publisher publisher = check new ({
        connection: sandboxBearerConnection(),
        topic: SANDBOX_EVENT_TOPIC
    });
    _ = check publisher->getTopic();
}

@test:Config {groups: ["sandbox"], enable: pubsubSandboxTests}
function testSandboxPublishPlatformEvent() returns error? {
    Publisher publisher = check new ({connection: sandboxBearerConnection(), topic: SANDBOX_EVENT_TOPIC});
    error? lastError = ();
    int attempt = 0;
    while attempt < 3 {
        string runId = "pubsub-sandbox-" + uuid:createType4AsString();
        string correlationId = uuid:createType4AsString();
        PublishResult[]|error outcome = publisher->publish([{
            id: correlationId,
            payload: {"CreatedDate": 0, "CreatedById": "", "Order_Id__c": runId, "Status__c": "CREATED"}
        }]);
        if outcome is PublishResult[] {
            test:assertEquals(outcome.length(), 1);
            test:assertEquals(outcome[0].id, correlationId);
            test:assertEquals(outcome[0].itemError, ());
            return;
        }
        lastError = outcome;
        attempt += 1;
        runtime:sleep(<decimal>attempt);
    }
    return lastError ?: error("platform-event publish did not return a result");
}

// Diagnostic boundary test: this uses the connector's dynamic payload encoder
// and generated unary client without Publisher's schema-cache and batching
// path. It is intentionally kept separate from the public Publisher test to
// identify which layer produces a request Salesforce rejects.
@test:Config {groups: ["sandbox"], enable: pubsubSandboxTests}
function testSandboxDirectPlatformEventPublish() returns error? {
    pubsubApi:PubSubClient pubSubClient = check new (DEFAULT_PUBSUB_ENDPOINT, {timeout: 30});
    map<string|string[]> headers = check metadataFor(sandboxBearerConnection(), sandboxAccessToken);
    pubsubApi:TopicInfo topic = check pubSubClient->GetTopic({content: {topic_name: SANDBOX_EVENT_TOPIC}, headers});
    pubsubApi:SchemaInfo schema = check pubSubClient->GetSchema({content: {schema_id: topic.schema_id}, headers});
    string correlationId = "pubsub-" + uuid:createType4AsString().substring(0, 29);
    byte[] encodedPayload = check encodePayload(schema.schema_json, {
        "CreatedDate": 0,
        "CreatedById": "",
        "Order_Id__c": "SPIKE-001",
        "Status__c": "CREATED"
    });
    test:assertEquals(encodedPayload.length(), 22);
    test:assertEquals(encodedPayload, [0, 0, 2, 18, 83, 80, 73, 75, 69, 45, 48, 48, 49, 2, 14,
        67, 82, 69, 65, 84, 69, 68]);
    pubsubApi:PublishResponse response = check pubSubClient->Publish({
        content: {
            topic_name: SANDBOX_EVENT_TOPIC,
            events: [{id: correlationId, schema_id: topic.schema_id, payload: encodedPayload}]
        },
        headers
    });
    test:assertEquals(response.results.length(), 1);
    test:assertEquals(response.results[0].correlation_key, correlationId);
    test:assertEquals(response.results[0].'error.msg, "");
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
    string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
    setSandboxExpectedAccountName(runId);
    resetSandboxCdcReceived();
    lock { sandboxCdcEventsSeen = 0; }
    Listener cdcListener = check new ({
        connection: sandboxBearerConnection(),
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            countSandboxCdcEvent();
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
    // Not `check`ed: a failed create must not skip the Listener stop below and
    // leave a live gRPC stream running past this test.
    salesforce:CreationResponse|error created = rest->create("Account", {"Name": runId});
    int elapsed = 0;
    boolean received = false;
    if created is salesforce:CreationResponse {
        while elapsed < SANDBOX_CDC_WAIT_SECONDS {
            received = didReceiveSandboxCdc();
            if received {
                break;
            }
            runtime:sleep(1);
            elapsed += 1;
        }
    }
    ListenerError? lastError = cdcListener.getLastError();
    // Stop before deleting, and regardless of whether the create above
    // succeeded: the cleanup delete must never satisfy this test.
    error? stopError = cdcListener.immediateStop();
    if created is error {
        return error("failed to create sandbox Account: " + created.message());
    }
    error? cleanup = rest->delete("Account", created.id);
    if stopError is error {
        return error("failed to stop sandbox CDC listener");
    }
    if cleanup is error {
        return error("failed to clean up sandbox Account");
    }
    if lastError is ListenerError {
        test:assertFail("listener terminated before delivery: operation=" + lastError.operation +
            " grpcStatus=" + (lastError.grpcStatus ?: "()") + " cause=" + lastError.cause.message());
    }
    test:assertTrue(received, "did not receive the Account CDC create event before timeout (saw " +
        sandboxCdcEventsSeenCount().toString() + " total CDC events on the channel during the wait)");
}
