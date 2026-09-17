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

// The remaining 9 scenarios from the approved 10-scenario CDC plan (scenario
// 1, plain CREATE delivery, is testSandboxAccountCdcLifecycle in
// sandbox_test.bal). Each scenario builds its own SandboxCdcCollector and
// handler, then runs through the shared runSandboxCdcScenario lifecycle
// helper so a Listener is never left running after a scenario returns,
// success or failure. Every scenario cleans up its own Account(s)
// unconditionally, mirroring scenario 1's established pattern, before
// surfacing any scenario error.

import ballerina/lang.runtime;
import ballerina/test;
import ballerina/uuid;
import ballerinax/salesforce;

// Salesforce documents closing an idle Subscribe stream (no pending
// FetchRequest credit exchanged) after roughly 60s. This clears that with
// margin so the connector's own reconnect logic -- not test timing luck --
// is what's actually being exercised.
final int SANDBOX_IDLE_RECONNECT_WAIT_SECONDS = 65;

// Scenario 2: UPDATE event delivery with correct record ID and changed value.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcUpdateDelivery() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario(SANDBOX_CDC_TOPIC, {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}},
            handler, function() returns error? {
        string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        salesforce:CreationResponse created = check rest->create("Account", {"Name": runId});
        string accountId = created.id;
        string updatedPhone = "555" + uuid:createType4AsString().substring(0, 7);
        error? scenarioResult = ();
        error? updateResult = rest->update("Account", accountId, {"Phone": updatedPhone});
        if updateResult is error {
            scenarioResult = updateResult;
        } else {
            CapturedCdcEvent? updateEvent = waitForChangeEvent(collector, accountId, "UPDATE", SANDBOX_CDC_WAIT_SECONDS);
            if updateEvent is () {
                scenarioResult = error("did not receive Account UPDATE CDC event before timeout; captured=" +
                    collector.snapshot().toString());
            } else if updateEvent.changedData["Phone"] != updatedPhone {
                scenarioResult = error("UPDATE changedData.Phone mismatch: expected " + updatedPhone +
                    " got " + updateEvent.changedData["Phone"].toString());
            }
        }
        error? cleanup = rest->delete("Account", accountId);
        if scenarioResult is error {
            return scenarioResult;
        }
        return cleanup;
    });
}

// Scenario 3: DELETE event delivery with correct metadata. DELETE events
// carry no field values in changedData (only CREATE/UPDATE populate it), so
// this matches purely by recordIds membership -- deleting the Account both
// triggers the event under test and is this scenario's own cleanup.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcDeleteDelivery() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario(SANDBOX_CDC_TOPIC, {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}},
            handler, function() returns error? {
        string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        salesforce:CreationResponse created = check rest->create("Account", {"Name": runId});
        string accountId = created.id;
        check rest->delete("Account", accountId);
        CapturedCdcEvent? deleteEvent = waitForChangeEvent(collector, accountId, "DELETE", SANDBOX_CDC_WAIT_SECONDS);
        if deleteEvent is () {
            return error("did not receive Account DELETE CDC event before timeout; captured=" +
                collector.snapshot().toString());
        }
        if deleteEvent.recordIds.indexOf(accountId) is () {
            return error("DELETE event recordIds did not include the deleted Account ID");
        }
    });
}

// Scenario 4: UNDELETE event delivery after restoring a deleted Account.
// Salesforce's REST API has no direct undelete resource (confirmed
// empirically -- composite/sobjects/undelete returns 404); the supported
// path is the Tooling API's executeAnonymous running Database.undelete(),
// which sandboxUndeleteAccount wraps.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcUndeleteDelivery() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario(SANDBOX_CDC_TOPIC, {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}},
            handler, function() returns error? {
        string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        salesforce:CreationResponse created = check rest->create("Account", {"Name": runId});
        string accountId = created.id;
        error? scenarioResult = ();
        check rest->delete("Account", accountId);
        error? undeleteResult = sandboxUndeleteAccount(accountId);
        if undeleteResult is error {
            scenarioResult = undeleteResult;
        } else {
            CapturedCdcEvent? undeleteEvent = waitForChangeEvent(collector, accountId, "UNDELETE",
                    SANDBOX_CDC_WAIT_SECONDS);
            if undeleteEvent is () {
                scenarioResult = error("did not receive Account UNDELETE CDC event before timeout; captured=" +
                    collector.snapshot().toString());
            } else if undeleteEvent.recordIds.indexOf(accountId) is () {
                scenarioResult = error("UNDELETE event recordIds did not include the restored Account ID");
            }
        }
        // Clean up regardless of restore outcome so this run leaves nothing behind.
        error? cleanup = rest->delete("Account", accountId);
        if scenarioResult is error {
            return scenarioResult;
        }
        return cleanup;
    });
}

// Scenario 5: one UPDATE that both sets a new value and clears (nulls)
// another field. Account is created with Phone already set so the same
// update can null it out alongside setting Website.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcChangedAndNulledFieldNormalization() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario(SANDBOX_CDC_TOPIC, {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}},
            handler, function() returns error? {
        string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        salesforce:CreationResponse created = check rest->create("Account", {"Name": runId, "Phone": "5550000000"});
        string accountId = created.id;
        string website = "https://example.com/" + uuid:createType4AsString();
        error? scenarioResult = ();
        error? updateResult = rest->update("Account", accountId, {"Website": website, "Phone": ()});
        if updateResult is error {
            scenarioResult = updateResult;
        } else {
            CapturedCdcEvent? updateEvent = waitForChangeEvent(collector, accountId, "UPDATE",
                    SANDBOX_CDC_WAIT_SECONDS);
            if updateEvent is () {
                scenarioResult = error("did not receive Account UPDATE CDC event before timeout; captured=" +
                    collector.snapshot().toString());
            } else if updateEvent.changedData["Website"] != website {
                scenarioResult = error("changedData.Website mismatch: expected " + website +
                    " got " + updateEvent.changedData["Website"].toString());
            } else if !updateEvent.changedData.hasKey("Phone") {
                scenarioResult = error("changedData is missing the nulled Phone field entirely");
            } else if updateEvent.changedData["Phone"] != () {
                scenarioResult = error("changedData.Phone should be null after clearing it, got " +
                    updateEvent.changedData["Phone"].toString());
            }
        }
        error? cleanup = rest->delete("Account", accountId);
        if scenarioResult is error {
            return scenarioResult;
        }
        return cleanup;
    });
}

// Scenario 6: three sequential updates to one Account, asserting the UPDATE
// events are delivered in the same order the changes were made -- not just
// that all three eventually arrive.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcSequentialUpdatesPreserveOrder() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario(SANDBOX_CDC_TOPIC, {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}},
            handler, function() returns error? {
        string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        salesforce:CreationResponse created = check rest->create("Account", {"Name": runId});
        string accountId = created.id;
        string[] sequence = ["555" + uuid:createType4AsString().substring(0, 7),
            "555" + uuid:createType4AsString().substring(0, 7), "555" + uuid:createType4AsString().substring(0, 7)];
        error? scenarioResult = ();
        foreach string phone in sequence {
            error? updateResult = rest->update("Account", accountId, {"Phone": phone});
            if updateResult is error {
                scenarioResult = updateResult;
                break;
            }
        }
        if scenarioResult is () {
            // CREATE (no Phone) + 3 UPDATEs (each with Phone) for this record.
            CapturedCdcEvent[] observed = waitForAtLeastNEventsForRecord(collector, accountId, 4,
                    SANDBOX_CDC_WAIT_SECONDS);
            string[] observedPhoneSequence = from CapturedCdcEvent captured in observed
                where captured.changeType == "UPDATE"
                select <string>captured.changedData["Phone"];
            if observedPhoneSequence != sequence {
                scenarioResult = error("UPDATE delivery order mismatch: expected " + sequence.toString() +
                    " got " + observedPhoneSequence.toString());
            }
        }
        error? cleanup = rest->delete("Account", accountId);
        if scenarioResult is error {
            return scenarioResult;
        }
        return cleanup;
    });
}

// Scenario 7: a burst of correlated Account creates, asserting every one is
// eventually delivered -- no event silently dropped under a rapid batch.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcBurstDeliveryNoLoss() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario(SANDBOX_CDC_TOPIC, {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}},
            handler, function() returns error? {
        int burstSize = 5;
        string[] accountIds = [];
        error? scenarioResult = ();
        foreach int i in 0 ..< burstSize {
            string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
            salesforce:CreationResponse|error created = rest->create("Account", {"Name": runId});
            if created is error {
                scenarioResult = created;
                break;
            }
            accountIds.push(created.id);
        }
        if scenarioResult is () {
            boolean allArrived = waitForAllChangeEvents(collector, accountIds, "CREATE", SANDBOX_CDC_WAIT_SECONDS);
            if !allArrived {
                scenarioResult = error("burst delivery incomplete: expected CREATE for all " +
                    burstSize.toString() + " accounts, captured=" + collector.snapshot().toString());
            }
        }
        // Clean up whatever accounts were actually created, regardless of outcome.
        error? cleanupResult = ();
        foreach string id in accountIds {
            error? deleted = rest->delete("Account", id);
            if deleted is error {
                cleanupResult = deleted;
            }
        }
        if scenarioResult is error {
            return scenarioResult;
        }
        return cleanupResult;
    });
}

// Scenario 8: publish event A, fail its handler once, then publish events B
// and C. Asserts A is redelivered and eventually succeeds, and that B/C are
// still delivered afterward in order -- neither lost nor overtaking the
// still-unresolved A while it retries.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcHandlerFailureThenContinuity() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    SandboxFailOnceGate gate = new;
    string runIdA = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            anydata? changedData = event.payload["changedData"];
            if changedData is map<anydata> && changedData["Name"] == runIdA && gate.shouldFailOnce() {
                return error("simulated handler failure for event A");
            }
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario(SANDBOX_CDC_TOPIC, {handlerRetry: {maxRetries: 2, initialDelay: 1},
            reconnectRetry: {maxRetries: 0}}, handler, function() returns error? {
        string runIdB = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        string runIdC = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        string[] createdIds = [];
        error? scenarioResult = ();
        salesforce:CreationResponse|error createdA = rest->create("Account", {"Name": runIdA});
        if createdA is error {
            scenarioResult = createdA;
        } else {
            createdIds.push(createdA.id);
            salesforce:CreationResponse|error createdB = rest->create("Account", {"Name": runIdB});
            salesforce:CreationResponse|error createdC = rest->create("Account", {"Name": runIdC});
            if createdB is error {
                scenarioResult = createdB;
            } else if createdC is error {
                scenarioResult = createdC;
            } else {
                createdIds.push(createdB.id);
                createdIds.push(createdC.id);
                boolean allArrived = waitForAllChangeEvents(collector, createdIds, "CREATE",
                        SANDBOX_CDC_WAIT_SECONDS);
                if !allArrived {
                    scenarioResult = error(
                        "handler-failure continuity: not all of A/B/C CREATE events arrived; captured=" +
                        collector.snapshot().toString());
                } else {
                    string[] arrivalOrder = from CapturedCdcEvent captured in collector.snapshot()
                        where captured.changeType == "CREATE"
                        select <string>captured.changedData["Name"];
                    int? indexA = arrivalOrder.indexOf(runIdA);
                    int? indexB = arrivalOrder.indexOf(runIdB);
                    int? indexC = arrivalOrder.indexOf(runIdC);
                    if indexA is () || indexB is () || indexC is () || !(indexA < indexB && indexB < indexC) {
                        scenarioResult = error("delivery order violated: expected A before B before C, got " +
                            arrivalOrder.toString());
                    }
                }
            }
        }
        error? cleanupResult = ();
        foreach string id in createdIds {
            error? deleted = rest->delete("Account", id);
            if deleted is error {
                cleanupResult = deleted;
            }
        }
        if scenarioResult is error {
            return scenarioResult;
        }
        return cleanupResult;
    });
}

// Scenario 10: CREATE delivery on the object-specific channel
// (/data/AccountChangeEvent) rather than the aggregate /data/ChangeEvents
// channel used by every other scenario in this file.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcObjectSpecificChannelDelivery() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario("/data/AccountChangeEvent",
            {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}, handler, function() returns error? {
        string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        salesforce:CreationResponse created = check rest->create("Account", {"Name": runId});
        string accountId = created.id;
        error? scenarioResult = ();
        CapturedCdcEvent? createEvent = waitForChangeEvent(collector, accountId, "CREATE", SANDBOX_CDC_WAIT_SECONDS);
        if createEvent is () {
            scenarioResult = error(
                "did not receive Account CREATE CDC event on object-specific channel before timeout; captured=" +
                collector.snapshot().toString());
        } else if createEvent.changedData["Name"] != runId {
            scenarioResult = error("CREATE changedData.Name mismatch on object-specific channel: expected " +
                runId + " got " + createEvent.changedData["Name"].toString());
        }
        error? cleanup = rest->delete("Account", accountId);
        if scenarioResult is error {
            return scenarioResult;
        }
        return cleanup;
    });
}

// Scenario 9: automatic Subscribe-stream reconnection after Salesforce's
// idle-stream closure, followed by successful CDC delivery. Uses default
// subscriptionConfig (not the fail-fast {maxRetries: 0} the other scenarios
// use) since the reconnect path under test is exactly what a default
// reconnectRetry budget is for.
@test:Config {groups: ["sandbox-cdc"], enable: pubsubSandboxTests}
function testSandboxCdcReconnectsAfterIdleStreamClosure() returns error? {
    salesforce:Client rest = check sandboxRestClient();
    SandboxCdcCollector collector = new;
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
            collector.capture(event);
        }
    };
    return runSandboxCdcScenario(SANDBOX_CDC_TOPIC, {}, handler, function() returns error? {
        runtime:sleep(<decimal>SANDBOX_IDLE_RECONNECT_WAIT_SECONDS);
        string runId = SANDBOX_ACCOUNT_PREFIX + uuid:createType4AsString();
        salesforce:CreationResponse created = check rest->create("Account", {"Name": runId});
        string accountId = created.id;
        error? scenarioResult = ();
        CapturedCdcEvent? createEvent = waitForChangeEvent(collector, accountId, "CREATE", SANDBOX_CDC_WAIT_SECONDS);
        if createEvent is () {
            scenarioResult = error(
                "did not receive Account CREATE CDC event after idle-stream reconnect; captured=" +
                collector.snapshot().toString());
        }
        error? cleanup = rest->delete("Account", accountId);
        if scenarioResult is error {
            return scenarioResult;
        }
        return cleanup;
    });
}
