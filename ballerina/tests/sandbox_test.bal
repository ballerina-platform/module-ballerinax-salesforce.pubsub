import ballerina/http;
import ballerina/lang.runtime;
import ballerina/test;
import ballerina/uuid;
import ballerinax/salesforce;

// The runner supplies these values only for an explicitly requested sandbox run.
public configurable boolean pubsubSandboxTests = false;
public configurable string sandboxEndpoint = "";
public configurable string sandboxAccessToken = "";
public configurable string sandboxClientId = "";
public configurable string sandboxClientSecret = "";
public configurable string sandboxRefreshUrl = "";
public configurable string sandboxUsername = "";
public configurable string sandboxPassword = "";
public configurable string sandboxTenantId = "";

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
