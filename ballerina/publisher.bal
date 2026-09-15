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

import ballerinax/salesforce.pubsub.internal as pubsubApi;
import ballerina/uuid;

# Publishes a batch of events to one configured Salesforce topic. Transport
# operations are added behind this topic-bound public abstraction.
public isolated client class Publisher {
    private final string topic;
    private final int targetRequestSizeBytes;
    private final readonly & ConnectionIdentity connection;
    private final PubSubTokenManager tokenManager;
    private final pubsubApi:PubSubClient pubsubClient;

    # Creates a topic-bound Publisher after local validation.
    #
    # + config - publisher configuration
    # + return - configuration error when the publisher cannot be constructed
    public isolated function init(PublisherConfig config) returns error? {
        check validatePublisherConfig(config);
        self.topic = config.topic;
        self.targetRequestSizeBytes = config.targetRequestSizeBytes;
        self.connection = <readonly & ConnectionIdentity>{
            instanceUrl: config.connection.instanceUrl,
            tenantId: config.connection.tenantId
        };
        self.tokenManager = new (config.connection);
        self.pubsubClient = check new (config.connection.endpoint, grpcConfigFor(config.connection));
    }

    # Retrieves current metadata for this Publisher's bound topic.
    #
    # + return - current Salesforce topic metadata or a request-level error
    isolated remote function getTopic() returns pubsubApi:TopicInfo|error {
        string accessToken = check self.tokenManager.getAccessToken();
        map<string|string[]> headers = check metadataForIdentity(self.connection, accessToken);
        pubsubApi:TopicInfo|error result = self.pubsubClient->GetTopic({content: {topic_name: self.topic}, headers});
        if result is error && grpcStatusNameOf(result) == "UNAUTHENTICATED" {
            check self.tokenManager.invalidateAccessToken();
            string refreshedToken = check self.tokenManager.getAccessToken();
            map<string|string[]> refreshedHeaders = check metadataForIdentity(self.connection, refreshedToken);
            return self.pubsubClient->GetTopic({content: {topic_name: self.topic}, headers: refreshedHeaders});
        }
        return result;
    }

    # Publishes one batch to this Publisher's bound topic. A batch larger than
    # the configured target is split across multiple Publish RPCs, in order.
    # If one chunk's RPC is ambiguous, that chunk's IDs are reported as such
    # but definitive results from other chunks are still returned on the
    # error rather than discarded.
    #
    # + events - events to publish
    # + return - one definitive result per submitted event, or a request-level error
    isolated remote function publish(PublishEvent[] events) returns PublishResult[]|error {
        if events.length() == 0 {
            return [];
        }
        pubsubApi:TopicInfo topic = check self->getTopic();
        if !topic.can_publish {
            return error("topic does not support publishing");
        }
        PublishEvent[] identifiedEvents = ensurePublishEventIds(events);
        check validateUniquePublishEventIds(identifiedEvents);
        GrpcSchemaLoader schemaLoader = new (self.pubsubClient, self.connection, self.tokenManager);
        string schemaJson = check processSchemaFor(self.connection.tenantId, topic.schema_id, schemaLoader);
        PreparedPublishEvents prepared = prepareProducerEvents(schemaJson, topic.schema_id, identifiedEvents);
        if prepared.wireEvents.length() == 0 {
            return prepared.localFailures;
        }
        pubsubApi:ProducerEvent[][] chunks = chunkProducerEvents(self.topic, prepared.wireEvents,
            self.targetRequestSizeBytes);

        pubsubApi:PublishResult[] definitiveWireResults = [];
        PublishResult[] rejectedChunkResults = [];
        string[] ambiguousEventIds = [];
        foreach pubsubApi:ProducerEvent[] chunk in chunks {
            string accessToken = check self.tokenManager.getAccessToken();
            map<string|string[]> headers = check metadataForIdentity(self.connection, accessToken);
            pubsubApi:PublishResponse|error outcome = self.pubsubClient->Publish({
                content: {topic_name: self.topic, events: chunk}, headers
            });
            if outcome is error {
                if isAmbiguousPublishFailure(outcome) {
                    foreach pubsubApi:ProducerEvent event in chunk {
                        ambiguousEventIds.push(event.id);
                    }
                } else {
                    error cause = outcome;
                    foreach pubsubApi:ProducerEvent event in chunk {
                        rejectedChunkResults.push({id: event.id, itemError: cause});
                    }
                }
            } else {
                foreach pubsubApi:PublishResult wireResult in outcome.results {
                    definitiveWireResults.push(wireResult);
                }
            }
        }
        PublishResult[] localFailures = [...prepared.localFailures, ...rejectedChunkResults];
        if ambiguousEventIds.length() > 0 {
            PublishResult[] definitiveResults = check definitiveResultsExcluding(identifiedEvents,
                localFailures, definitiveWireResults, ambiguousEventIds);
            return ambiguousPublishError(self.topic, ambiguousEventIds, definitiveResults,
                error("one or more Publish RPCs ended without a definitive response"));
        }
        return mergePublisherResults(identifiedEvents, localFailures, definitiveWireResults);
    }
}

// Events that can be published together with definitive local failures. Local
// failures are deliberately separated so valid siblings still reach Publish.
type PreparedPublishEvents record {|
    pubsubApi:ProducerEvent[] wireEvents;
    PublishResult[] localFailures;
|};

// Retrieves schemas through the authenticated generated client only on a
// connector-wide cache miss. It is internal so gRPC types never cross the
// public connector boundary.
isolated class GrpcSchemaLoader {
    *SchemaLoader;
    private final pubsubApi:PubSubClient pubsubClient;
    private final readonly & ConnectionIdentity connection;
    private final PubSubTokenManager tokenManager;

    isolated function init(pubsubApi:PubSubClient pubsubClient, readonly & ConnectionIdentity connection,
            PubSubTokenManager tokenManager) {
        self.pubsubClient = pubsubClient;
        self.connection = connection;
        self.tokenManager = tokenManager;
    }

    public isolated function load(string tenantId, string schemaId) returns string|error {
        string accessToken = check self.tokenManager.getAccessToken();
        map<string|string[]> headers = check metadataForIdentity(self.connection, accessToken);
        pubsubApi:SchemaInfo|error result = self.pubsubClient->GetSchema({
            content: {schema_id: schemaId}, headers
        });
        if result is error && grpcStatusNameOf(result) == "UNAUTHENTICATED" {
            check self.tokenManager.invalidateAccessToken();
            string refreshedToken = check self.tokenManager.getAccessToken();
            map<string|string[]> refreshedHeaders = check metadataForIdentity(self.connection, refreshedToken);
            result = self.pubsubClient->GetSchema({content: {schema_id: schemaId}, headers: refreshedHeaders});
        }
        pubsubApi:SchemaInfo schema = check result;
        return schema.schema_json;
    }
}

# Adds connector-generated correlation IDs to events that omit one. Caller IDs
# are retained unchanged and are correlation values, not deduplication keys.
#
# + events - events submitted for one publish attempt
# + return - events with a non-empty ID for every element
public isolated function ensurePublishEventIds(PublishEvent[] events) returns PublishEvent[] {
    PublishEvent[] identified = [];
    foreach PublishEvent event in events {
        string? id = event.id;
        identified.push({payload: event.payload, id: id is string && id.length() > 0 ? id : uuid:createType4AsString()});
    }
    return identified;
}

// Rejects duplicate correlation IDs before Publish because a Salesforce result
// identifies the input event by this ID. Generated IDs are UUIDs and callers
// retain responsibility for making supplied IDs unique within a batch.
isolated function validateUniquePublishEventIds(PublishEvent[] events) returns error? {
    map<boolean> seen = {};
    foreach PublishEvent event in events {
        string id = event.id ?: "";
        if seen.hasKey(id) {
            return error("publish event IDs must be unique within a batch");
        }
        seen[id] = true;
    }
}

# Encodes identified public events into internal Publish RPC events using the
# current writer schema for the Publisher's topic.
#
# + schemaJson - Avro writer schema returned by GetSchema
# + schemaId - Salesforce schema identifier returned by GetTopic
# + events - caller events with generated or caller-provided IDs
# + return - generated transport events or an encoding error
public isolated function producerEventsFor(string schemaJson, string schemaId, PublishEvent[] events)
        returns pubsubApi:ProducerEvent[]|error {
    pubsubApi:ProducerEvent[] wireEvents = [];
    foreach PublishEvent event in ensurePublishEventIds(events) {
        byte[] payload = check encodePayload(schemaJson, event.payload);
        wireEvents.push({id: event.id ?: "", schema_id: schemaId, payload});
    }
    return wireEvents;
}

// Encodes all valid events and turns a local Avro failure into that event's
// item-level result. IDs must already be stable for result correlation.
isolated function prepareProducerEvents(string schemaJson, string schemaId, PublishEvent[] events)
        returns PreparedPublishEvents {
    pubsubApi:ProducerEvent[] wireEvents = [];
    PublishResult[] localFailures = [];
    foreach PublishEvent event in events {
        string id = event.id ?: "";
        byte[]|error payload = encodePayload(schemaJson, event.payload);
        if payload is byte[] {
            wireEvents.push({id, schema_id: schemaId, payload});
        } else {
            localFailures.push({id, itemError: payload});
        }
    }
    return {wireEvents, localFailures};
}

// Returns the encoded protobuf size of an event, including field tags and
// length prefixes. The encoder omits scalar fields whose byte value is empty.
isolated function producerEventWireSize(pubsubApi:ProducerEvent event) returns int {
    int size = protobufBytesFieldSize(event.id.toBytes().length());
    size += protobufBytesFieldSize(event.schema_id.toBytes().length());
    size += protobufBytesFieldSize(event.payload.length());
    foreach pubsubApi:EventHeader header in event.headers {
        int headerSize = protobufBytesFieldSize(header.key.toBytes().length())
            + protobufBytesFieldSize(header.value.length());
        size += 1 + protobufVarintSize(headerSize) + headerSize;
    }
    return size;
}

// Returns how much one event adds to a Publish request's encoded size,
// including the repeated-field tag and length prefix that wrap it.
isolated function producerEventRequestContribution(pubsubApi:ProducerEvent event) returns int {
    int eventSize = producerEventWireSize(event);
    return 1 + protobufVarintSize(eventSize) + eventSize;
}

# Splits encoded events across as many Publish requests as needed to keep
# each at or under the configured target size, preserving input order across
# and within chunks. A single event whose own contribution already reaches
# the target still gets its own one-event chunk rather than being dropped.
#
# + topic - canonical topic included in every request's size accounting
# + events - already-encoded events in caller order
# + targetRequestSizeBytes - soft per-request size target
# + return - one or more chunks, each in original relative order
public isolated function chunkProducerEvents(string topic, pubsubApi:ProducerEvent[] events,
        int targetRequestSizeBytes) returns pubsubApi:ProducerEvent[][] {
    pubsubApi:ProducerEvent[][] chunks = [];
    int baseSize = protobufBytesFieldSize(topic.toBytes().length());
    pubsubApi:ProducerEvent[] currentChunk = [];
    int currentChunkSize = baseSize;
    foreach pubsubApi:ProducerEvent event in events {
        int eventContribution = producerEventRequestContribution(event);
        if currentChunk.length() > 0 && currentChunkSize + eventContribution > targetRequestSizeBytes {
            chunks.push(currentChunk);
            currentChunk = [];
            currentChunkSize = baseSize;
        }
        currentChunk.push(event);
        currentChunkSize += eventContribution;
    }
    if currentChunk.length() > 0 {
        chunks.push(currentChunk);
    }
    return chunks;
}

isolated function protobufBytesFieldSize(int valueLength) returns int {
    return valueLength == 0 ? 0 : 1 + protobufVarintSize(valueLength) + valueLength;
}

isolated function protobufVarintSize(int value) returns int {
    int bytes = 1;
    int remaining = value;
    while remaining >= 128 {
        remaining = remaining / 128;
        bytes += 1;
    }
    return bytes;
}

// Produces results in input order after combining local failures with the
// definitive Salesforce results. A missing or unexpected correlation key is a
// request-level protocol error because the caller cannot safely identify the
// affected event.
isolated function mergePublisherResults(PublishEvent[] events, PublishResult[] localFailures,
        pubsubApi:PublishResult[] wireResults) returns PublishResult[]|error {
    map<PublishResult> byId = {};
    map<boolean> submittedIds = {};
    foreach PublishEvent event in events {
        submittedIds[event.id ?: ""] = true;
    }
    foreach PublishResult localFailure in localFailures {
        byId[localFailure.id] = localFailure;
    }
    foreach PublishResult remoteResult in publisherResultsFor(wireResults) {
        if !submittedIds.hasKey(remoteResult.id) || byId.hasKey(remoteResult.id) {
            return error("Publish returned an unexpected or duplicate correlation ID");
        }
        byId[remoteResult.id] = remoteResult;
    }

    PublishResult[] results = [];
    foreach PublishEvent event in events {
        string id = event.id ?: "";
        PublishResult? result = byId[id];
        if result is PublishResult {
            results.push(result);
        } else {
            return error("Publish response omitted a submitted event result");
        }
    }
    return results;
}

// Builds definitive results for every submitted event except those named in
// `ambiguousIds`, in input order. Used only to populate the sibling results
// carried by an ambiguous-outcome error, so a missing result for an ambiguous
// ID is expected here, not a bug.
isolated function definitiveResultsExcluding(PublishEvent[] events, PublishResult[] localFailures,
        pubsubApi:PublishResult[] wireResults, string[] ambiguousIds) returns PublishResult[]|error {
    map<boolean> ambiguousIdSet = {};
    foreach string id in ambiguousIds {
        ambiguousIdSet[id] = true;
    }
    map<PublishResult> byId = {};
    map<boolean> submittedIds = {};
    foreach PublishEvent event in events {
        submittedIds[event.id ?: ""] = true;
    }
    foreach PublishResult localFailure in localFailures {
        byId[localFailure.id] = localFailure;
    }
    foreach PublishResult remoteResult in publisherResultsFor(wireResults) {
        if !submittedIds.hasKey(remoteResult.id) || byId.hasKey(remoteResult.id) {
            return error("Publish returned an unexpected or duplicate correlation ID");
        }
        byId[remoteResult.id] = remoteResult;
    }

    PublishResult[] results = [];
    foreach PublishEvent event in events {
        string id = event.id ?: "";
        PublishResult? result = byId[id];
        if result is PublishResult {
            results.push(result);
        } else if !ambiguousIdSet.hasKey(id) {
            return error("Publish response omitted a submitted event result");
        }
    }
    return results;
}

# Validates publisher settings before a Pub/Sub RPC is started.
#
# + config - publisher construction configuration
# + return - an error when the configuration cannot describe a valid request
public isolated function validatePublisherConfig(PublisherConfig config) returns error? {
    check validateConnectionConfig(config.connection);
    check validateRetryPolicy(config.retryPolicy);
    if config.topic.length() == 0 {
        return error("topic must not be empty");
    }
    if config.targetRequestSizeBytes <= 0 {
        return error("targetRequestSizeBytes must be greater than zero");
    }
}

# Converts definitive item-level Salesforce outcomes without changing their
# batch order. Transport failures remain request-level errors at the caller.
#
# + wireResults - Publish results returned by Salesforce
# + return - corresponding public item outcomes
public isolated function publisherResultsFor(pubsubApi:PublishResult[] wireResults) returns PublishResult[] {
    PublishResult[] results = [];
    foreach pubsubApi:PublishResult wireResult in wireResults {
        error? itemError = ();
        if wireResult.'error.code == pubsubApi:PUBLISH || wireResult.'error.msg.length() > 0 {
            itemError = error(wireResult.'error.msg);
        }
        results.push({
            id: wireResult.correlation_key,
            replayId: itemError is error ? () : wireResult.replay_id,
            itemError
        });
    }
    return results;
}
