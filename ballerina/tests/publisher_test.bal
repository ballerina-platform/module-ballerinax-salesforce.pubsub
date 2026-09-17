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
import ballerina/grpc;
import ballerina/test;
import ballerinax/salesforce.pubsub.internal as pubsubApi;

function publisherConfig() returns PublisherConfig {
    return {
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        },
        topic: "/event/Order_Notification__e"
    };
}

// This fails if a non-positive splitting target reaches the chunking logic,
// where it would either loop or silently fall back to unbounded requests.
@test:Config {}
function testPublisherConfigRejectsInvalidBatchLimits() {
    PublisherConfig config = publisherConfig();
    config.targetRequestSizeBytes = 0;
    error? result = validatePublisherConfig(config);

    test:assertTrue(result is error);
}

@test:Config {}
function testPublisherConfigRejectsInvalidRetryPolicy() {
    PublisherConfig config = publisherConfig();
    config.retryPolicy = {maxRetries: -1};

    test:assertTrue(validatePublisherConfig(config) is error);
}

// This fails if a Salesforce item error turns successful sibling events into a
// request-level failure or loses the response correlation key.
@test:Config {}
function testPublisherMapsPartialSalesforceResultsIndividually() {
    pubsubApi:PublishResult[] wireResults = [
        {correlation_key: "first", replay_id: [1, 2]},
        {correlation_key: "second", 'error: {code: pubsubApi:PUBLISH, msg: "validation failed"}}
    ];

    PublishResult[] results = publisherResultsFor(wireResults);
    test:assertEquals(results.length(), 2);
    test:assertEquals(results[0].id, "first");
    test:assertEquals(results[0].replayId, [1, 2]);
    test:assertEquals(results[0].itemError, ());
    test:assertEquals(results[1].id, "second");
    test:assertTrue(results[1].itemError is error);
}

// Results must stay aligned with original inputs when a locally invalid event
// sits between two events that Salesforce accepted.
@test:Config {}
function testPublisherMergesLocalAndRemoteResultsInInputOrder() returns error? {
    PublishEvent[] events = [
        {id: "first", payload: {}},
        {id: "local-failure", payload: {}},
        {id: "third", payload: {}}
    ];
    PublishResult[] localFailures = [{id: "local-failure", itemError: error("invalid enum")}];
    pubsubApi:PublishResult[] remoteResults = [
        {correlation_key: "first", replay_id: [1]},
        {correlation_key: "third", replay_id: [3]}
    ];

    PublishResult[] results = check mergePublisherResults(events, localFailures, remoteResults);
    test:assertEquals(results.length(), 3);
    test:assertEquals(results[0].id, "first");
    test:assertEquals(results[1].id, "local-failure");
    test:assertTrue(results[1].itemError is error);
    test:assertEquals(results[2].id, "third");
}

// Caller correlation IDs identify item results, so duplicate caller IDs would
// make a partial response ambiguous and must be rejected before any RPC.
@test:Config {}
function testPublisherRejectsDuplicateCorrelationIds() {
    error? result = validateUniquePublishEventIds([
        {id: "duplicate", payload: {}},
        {id: "duplicate", payload: {}}
    ]);

    test:assertTrue(result is error);
}

// Salesforce rejects an overlong ProducerEvent ID with a generic INTERNAL
// response. Keep that server-side constraint at the local validation boundary.
@test:Config {}
function testPublisherRejectsOverlongCorrelationIds() {
    error? result = validateUniquePublishEventIds([{
        id: "1234567890123456789012345678901234567",
        payload: {}
    }]);

    test:assertTrue(result is error);
}

// A lost Publish response is not retried. Its request-level error identifies
// exactly the IDs that may already have been accepted by Salesforce, without
// discarding definitive results already obtained for sibling events (for
// example, from another chunk of a split batch that succeeded).
@test:Config {}
function testAmbiguousPublishErrorIdentifiesSubmittedEvents() {
    PublishResult[] siblingResults = [{id: "second", replayId: [9]}];
    error<AmbiguousPublishDetail> result = ambiguousPublishError("/event/Order__e", ["first", "third"],
        siblingResults);
    AmbiguousPublishDetail detail = result.detail();
    test:assertEquals(result.message(), "ambiguous Publish outcome");
    test:assertEquals(detail.topic, "/event/Order__e");
    test:assertEquals(detail.eventIds, ["first", "third"]);
    test:assertEquals(detail.definitiveResults.length(), 1);
    test:assertEquals(detail.definitiveResults[0].id, "second");
    test:assertEquals(detail.definitiveResults[0].replayId, [9]);
    test:assertEquals(detail.definitiveResults[0].itemErrorMessage, ());
}

@test:Config {}
function testPublishFailureClassificationKeepsDefinitiveGrpcErrorsOutOfAmbiguity() {
    test:assertFalse(isAmbiguousPublishFailure(error grpc:UnauthenticatedError("expired")));
    test:assertFalse(isAmbiguousPublishFailure(error grpc:PermissionDeniedError("denied")));
    test:assertFalse(isAmbiguousPublishFailure(error grpc:InvalidArgumentError("invalid payload")));
    test:assertTrue(isAmbiguousPublishFailure(error grpc:UnavailableError("transport lost")));
}
