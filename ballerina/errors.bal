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

# Creates a public terminal-listener diagnostic without retaining credential or
# payload-bearing transport error text.
#
# + operation - failed connector operation
# + topic - affected canonical topic
# + cause - underlying transport failure
# + grpcStatus - optional gRPC status
# + salesforceErrorCode - optional Salesforce error code
# + rpcId - optional Salesforce RPC identifier
# + return - privacy-safe listener error context
public isolated function listenerErrorFor(string operation, string? topic, error cause, string? grpcStatus,
        string? salesforceErrorCode, string? rpcId) returns ListenerError {
    return {
        operation,
        topic,
        grpcStatus,
        salesforceErrorCode,
        rpcId,
        cause: error("Pub/Sub transport operation failed")
    };
}

# A definitive sibling result carried on an ambiguous-outcome error. Ballerina
# error detail fields must be `anydata`, which `PublishResult` is not (its
# `itemError` is `error?`), so this mirrors `PublishResult` with the item
# error reduced to its message text.
public type AmbiguousPublishSiblingResult record {|
    # Caller or connector-generated correlation ID.
    string id;
    # Salesforce replay ID when an event was accepted.
    byte[]? replayId = ();
    # Message of a definitive local or Salesforce item-level failure, if any.
    string? itemErrorMessage = ();
|};

# Detail retained for an uncertain unary Publish result. Callers can use the
# IDs to reconcile with their own idempotency records without the connector
# making a potentially duplicate publish attempt.
public type AmbiguousPublishDetail record {|
    # Topic passed to the uncertain Publish request.
    string topic;
    # Correlation IDs of events whose Publish RPC ended without a definitive
    # response. A large batch split across several requests can have some
    # ambiguous chunks alongside others that got a definitive answer; only the
    # ambiguous ones are named here.
    string[] eventIds;
    # Definitive results for every other submitted event, in input order. A
    # split batch's other chunks may have succeeded, or failed, definitively
    # even though this error is returned for the batch as a whole; these
    # results are not discarded.
    AmbiguousPublishSiblingResult[] definitiveResults;
|};

# Creates the request-level error returned when one or more Publish RPCs in a
# batch (a batch is one or more requests once split by size) ended without a
# definitive response from Salesforce.
#
# + topic - topic passed to the uncertain request
# + eventIds - correlation IDs whose Publish RPC was ambiguous
# + definitiveResults - results already obtained for every other submitted event
# + return - structured request-level ambiguity error
public isolated function ambiguousPublishError(string topic, string[] eventIds,
        PublishResult[] definitiveResults) returns error<AmbiguousPublishDetail> {
    AmbiguousPublishSiblingResult[] siblings = [];
    foreach PublishResult result in definitiveResults {
        error? itemError = result.itemError;
        siblings.push({
            id: result.id,
            replayId: result.replayId,
            itemErrorMessage: itemError is error ? itemError.message() : ()
        });
    }
    return error("ambiguous Publish outcome", topic = topic, eventIds = eventIds, definitiveResults = siblings);
}
