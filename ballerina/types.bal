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

# A dynamically decoded Avro event body. Salesforce schemas are retrieved at
# runtime, so applications own any conversion to a static record type.
public type Payload record {};

# A binary-safe event header. An array is used in Event to preserve repeated
# header keys from the Salesforce wire contract.
public type Header record {|
    # Header name.
    string key;
    # Header value without text conversion.
    byte[] value;
|};

# A decoded event delivered to a Listener service.
public type Event record {|
    # Canonical Salesforce topic path.
    string topic;
    # Opaque Salesforce replay identifier.
    byte[] replayId;
    # Writer schema used to decode payload.
    string schemaId;
    # Salesforce event ID when the event family supplies one.
    string? eventId = ();
    # Headers in wire order, including repeated keys. Salesforce marks this
    # field reserved for future use (for example distributed-tracing headers)
    # and does not currently populate it, so this is typically empty.
    Header[] headers = [];
    # Runtime-schema decoded event body.
    Payload payload;
|};

# A caller-supplied event for unary publication.
public type PublishEvent record {|
    # Dynamic event body to encode using the topic schema.
    Payload payload;
    # Caller correlation ID. The connector generates one when omitted.
    string? id = ();
|};

# The outcome of one input event in a publish batch.
public type PublishResult record {|
    # Caller or connector-generated correlation ID.
    string id;
    # Salesforce replay ID when an event was accepted.
    byte[]? replayId = ();
    # Definitive local or Salesforce item-level failure.
    error? itemError = ();
|};

# Initial or recovery starting position for a Subscribe stream.
public enum ReplayPosition {
    LATEST,
    EARLIEST
}

# Bounded retry controls shared by handler and transport recovery.
public type RetryPolicy record {|
    # Number of retries after the initial attempt.
    int maxRetries = 3;
    # Delay before the first retry, in seconds.
    decimal initialDelay = 1;
    # Upper bound for a retry delay, in seconds.
    decimal maxDelay = 30;
|};

# Topic-scoped sequential delivery configuration. Attach as `@pubsub:ServiceConfig`
# on a service declaration to override `ListenerConfig.subscriptionConfig` for
# that topic. An override replaces the whole record: any field the annotation
# omits takes this type's own default, not `subscriptionConfig`'s value.
public type SubscriptionConfig record {|
    # Position used when no checkpoint exists.
    ReplayPosition initialReplay = LATEST;
    # Position used when Salesforce rejects an expired checkpoint.
    ReplayPosition expiredReplayRecovery = EARLIEST;
    # Normal maximum number of events awaiting sequential processing.
    int bufferSize = 10;
    # Retry configuration for handler failures.
    RetryPolicy handlerRetry = {};
    # Retry configuration for reconnectable stream failures.
    RetryPolicy reconnectRetry = {};
|};

# Publisher construction settings for one canonical Salesforce topic.
public type PublisherConfig record {|
    # Shared authenticated transport configuration.
    ConnectionConfig connection;
    # Canonical publishable Salesforce topic path.
    string topic;
    # Soft target for one Publish request's encoded size; a larger batch is
    # split across multiple Publish RPCs. Keep well below Salesforce's 4 MB
    # per-request hard limit, which this connector does not itself enforce.
    int targetRequestSizeBytes = 3 * 1024 * 1024;
    # Retry settings reserved for eligible metadata operations. Publish calls
    # themselves are not automatically retried after an ambiguous outcome.
    RetryPolicy retryPolicy = {};
|};

# Listener construction settings shared by all attached topic services.
public type ListenerConfig record {|
    # Shared authenticated transport configuration.
    ConnectionConfig connection;
    # Name that distinguishes independent consumer applications/processes that
    # may share a durable replay store. Shared by every topic this Listener
    # subscribes to; a Listener already rejects two of its own services sharing
    # one topic, so `{tenantId, topic, logicalSubscriptionName}` stays unique
    # per topic without a per-topic value.
    string logicalSubscriptionName = "default";
    # Defaults applied to a topic whose service carries no @ServiceConfig
    # annotation.
    SubscriptionConfig subscriptionConfig = {};
    # Cursor store shared by attached topic subscriptions. The default survives
    # only while this Listener process remains alive.
    ReplayStore replayStore = new InMemoryReplayStore();
|};

# Service contract for one canonical Pub/Sub topic. `onEvent` is invoked in
# topic order; an `onError` callback remains optional and is handled by the
# Listener lifecycle rather than required by this service type.
public type Service service object {
    remote function onEvent(Event event) returns error?;
};

# Per-service override of `ListenerConfig.subscriptionConfig` for one attached
# topic. Presence of this annotation replaces the entire effective
# `SubscriptionConfig` for that topic; fields the annotation omits take
# `SubscriptionConfig`'s own defaults, not `subscriptionConfig`'s values.
public annotation SubscriptionConfig ServiceConfig on service;

# Privacy-safe terminal listener failure context.
public type ListenerError record {|
    # Operation that failed, such as `Subscribe` or `GetSchema`.
    string operation;
    # Topic affected by the failure when known.
    string? topic = ();
    # gRPC status name (for example `UNAVAILABLE`), when the failure carries one.
    string? grpcStatus = ();
    # Always `()` in V1: the generated streaming client does not expose response
    # trailers on a failed receive, so no Salesforce-specific error code is available.
    string? salesforceErrorCode = ();
    # Always `()` in V1; see `salesforceErrorCode`.
    string? rpcId = ();
    # Underlying connector error without credentials or payloads.
    error cause;
|};
