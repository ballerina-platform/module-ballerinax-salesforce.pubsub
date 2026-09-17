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

import ballerinax/salesforce.pubsub.internal as wire;
import ballerina/lang.runtime;

// Connector-owned per D21; not exposed as configuration.
final decimal GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 5;

type ShutdownTimedOut "graceful-shutdown-timeout";

isolated function sleepThenTimeout(decimal seconds) returns ShutdownTimedOut {
    runtime:sleep(seconds);
    return "graceful-shutdown-timeout";
}

// Races a topic worker's completion against a bounded timeout so shutdown
// never blocks indefinitely on a stuck handler or stream read.
function waitForWorker(future<error?> workerFuture, decimal timeoutSeconds) returns error?|ShutdownTimedOut {
    future<ShutdownTimedOut> timeoutFuture = start sleepThenTimeout(timeoutSeconds);
    error?|ShutdownTimedOut result = wait workerFuture | timeoutFuture;
    return result;
}

// Reassembles the path segments the compiler supplies for a multi-segment
// declarative `service /event/X on listener` declaration back into one
// canonical topic string. Confirmed empirically (not assumed): the compiler
// passes each segment separately with no leading empty segment for the root
// `/` and no `/` characters embedded in an element.
isolated function joinAttachPathSegments(string[] segments) returns string {
    string joined = "";
    foreach string segment in segments {
        joined += "/" + segment;
    }
    return joined;
}

type ActiveSubscription record {|
    wire:SubscribeStreamingClient streamClient;
    FlowController flow;
    DeliveryGate gate;
    ReplayKey replayKey;
    SubscriptionConfig config;
    Service attachedService;
|};

class ServiceEventHandler {
    *EventHandler;
    private final Service target;

    function init(Service target) {
        self.target = target;
    }

    public function onEvent(Event event) returns error? {
        return self.target->onEvent(event);
    }
}

// Spelled out in full rather than built with `*Service;` type inclusion:
// Ballerina rejects a `->` remote call on a value whose static type reaches
// `onEvent`/`onError` through object type inclusion, even when narrowed from
// a `Service` value that structurally has both methods. Only a field of this
// exact, fully-declared type supports the call.
type ErrorAwareService service object {
    remote function onEvent(Event event) returns error?;
    remote function onError(ListenerError err) returns error?;
};

class ServiceErrorNotifier {
    private final ErrorAwareService target;

    function init(ErrorAwareService target) {
        self.target = target;
    }

    public function notify(ListenerError err) returns error? {
        return self.target->onError(err);
    }
}

# Owns the services and independent topic subscriptions for one Salesforce
# Pub/Sub connection. Construction and attachment are local; authentication and
# stream opening occur when the Listener starts.
public class Listener {
    private final ListenerConfig config;
    private final PubSubTokenManager tokenManager;
    private map<Service> services = {};
    private map<ActiveSubscription> subscriptions = {};
    private map<future<error?>> workers = {};
    // Workers whose shutdown wait timed out: Ballerina's alternate `wait`
    // never cancelled them for us, so they may still be running. Tracked here
    // so 'start() can confirm they have actually exited before reusing the
    // topic, instead of racing a fresh worker against a leaked one.
    private map<future<error?>> drainingWorkers = {};
    private wire:PubSubClient? pubsubClient = ();
    private boolean started = false;
    private ListenerError? terminalError = ();

    # Creates a Listener after validating configuration without opening a
    # network connection.
    public function init(ListenerConfig config) returns error? {
        check validateListenerConfig(config);
        self.config = config;
        self.tokenManager = new (config.connection);
    }

    # Registers a service for its canonical Salesforce topic. The compiler
    # supplies the service path as `name` for `service "..." on listener`,
    # including a declarative `service /event/X on listener` declaration —
    # a multi-segment absolute path arrives as its individual segments,
    # reassembled here into one canonical topic string.
    #
    # + s - service with a remote `onEvent` method
    # + name - canonical Salesforce topic path
    # + return - an error for an invalid or duplicate topic, or an invalid
    #   effective subscription configuration, including one supplied by a
    #   @ServiceConfig annotation
    public function attach(Service s, string[]|string? name = ()) returns error? {
        string topic;
        if name is string[] {
            if name.length() == 0 {
                return error("Listener service path must be one canonical topic string");
            }
            topic = joinAttachPathSegments(name);
        } else if name is string && name.length() > 0 {
            topic = name;
        } else {
            return error("Listener service path must be one canonical topic string");
        }
        SubscriptionConfig resolvedConfig = subscriptionConfigFor(self.config, s);
        check validateSubscriptionConfig(resolvedConfig);
        if self.services.hasKey(topic) {
            return error("a service is already attached for topic " + topic);
        }
        self.services[topic] = s;
    }

    # Detaches a service that was previously attached with attach(). A no-op
    # when the service is not currently attached. Detaching a topic whose
    # stream is already running is not supported in V1; call this only before
    # 'start().
    #
    # + s - the service value previously passed to attach()
    # + return - always `()` in V1
    public function detach(Service s) returns error? {
        foreach var [topic, attachedService] in self.services.entries() {
            if attachedService === s {
                _ = self.services.remove(topic);
                return;
            }
        }
    }

    # Opens an independent authenticated Subscribe stream for every attached
    # service. Streams begin from each topic's stored cursor or configured
    # initial replay position.
    #
    # + return - an error when topic metadata, authentication, or stream setup fails
    public function 'start() returns error? {
        if self.started {
            return ();
        }
        check self.fenceDrainingWorkers();
        wire:PubSubClient grpcClient = check new (self.config.connection.endpoint, grpcConfigFor(self.config.connection));
        self.pubsubClient = grpcClient;
        foreach var [topic, attachedService] in self.services.entries() {
            error? openError = self.openTopicSubscription(grpcClient, topic, attachedService);
            if openError is error {
                error? cleanupError = self.gracefulStop();
                if cleanupError is error {
                    return cleanupError;
                }
                return openError;
            }
        }
        self.started = true;
        self.workers = {};
        foreach string topic in self.subscriptions.keys() {
            self.workers[topic] = start self.consumeTopic(topic);
        }
    }

    # Stops replenishment and closes every active topic stream, then waits up
    # to a connector-owned timeout for each topic's in-flight handler and
    # checkpoint work to finish before returning. A replay ID is saved before a
    # replacement FetchRequest, so uncheckpointed work remains replayable after
    # a forced cancellation past the timeout.
    #
    # + return - an error when a stream cannot be closed or a worker fails
    public function gracefulStop() returns error? {
        return self.stopInternal(GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS);
    }

    # Stops every stream without waiting for in-flight handler or checkpoint
    # work to finish. The next start resumes from each last durable replay
    # checkpoint; uncheckpointed work in progress at the moment of the call may
    # be redelivered.
    #
    # + return - an error when a stream cannot be closed
    public function immediateStop() returns error? {
        return self.stopInternal(0);
    }

    private function stopInternal(decimal workerWaitTimeoutSeconds) returns error? {
        self.started = false;
        error? firstError = ();
        foreach var [_, subscription] in self.subscriptions.entries() {
            wire:SubscribeStreamingClient streamClient = subscription.streamClient;
            error? closeError = streamClient->complete();
            if closeError is error && firstError is () {
                firstError = closeError;
            }
        }
        // Each topic's wait races its own worker against the same timeout
        // concurrently, so the overall wait stays bounded by that one timeout
        // regardless of how many topics are attached, rather than multiplying
        // it by the topic count.
        map<future<error?|ShutdownTimedOut>> waiters = {};
        foreach var [topic, workerFuture] in self.workers.entries() {
            waiters[topic] = start waitForWorker(workerFuture, workerWaitTimeoutSeconds);
        }
        foreach var [topic, waiterFuture] in waiters.entries() {
            error?|ShutdownTimedOut outcome = wait waiterFuture;
            if outcome is ShutdownTimedOut {
                future<error?>? staleWorker = self.workers[topic];
                if staleWorker is future<error?> {
                    staleWorker.cancel();
                    self.drainingWorkers[topic] = staleWorker;
                }
            } else if outcome is error && firstError is () {
                firstError = outcome;
            }
        }
        self.subscriptions = {};
        self.workers = {};
        self.pubsubClient = ();
        if firstError is error {
            return firstError;
        }
    }

    // Confirms every topic left "draining" by a previous shutdown timeout has
    // actually exited before 'start() reuses it. Cancellation only sets a
    // cooperative flag the worker's strand checks the next time it yields, so
    // this waits again -- bounded by the same shutdown timeout -- rather than
    // assuming cancel() already stopped it.
    private function fenceDrainingWorkers() returns error? {
        if self.drainingWorkers.length() == 0 {
            return;
        }
        string[] exited = [];
        string leaked = "";
        foreach var [topic, staleWorker] in self.drainingWorkers.entries() {
            error?|ShutdownTimedOut outcome = waitForWorker(staleWorker, GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS);
            if outcome is ShutdownTimedOut {
                leaked = leaked.length() == 0 ? topic : leaked + ", " + topic;
            } else {
                exited.push(topic);
            }
        }
        foreach string topic in exited {
            _ = self.drainingWorkers.remove(topic);
        }
        if leaked.length() > 0 {
            return error("cannot start: a worker for topic(s) " + leaked +
                " has not exited since a previous shutdown timed out; it may be leaked");
        }
    }

    # Returns the privacy-safe terminal failure that stopped this Listener, if
    # one has occurred. Normal application errors and credentials are never
    # retained in this diagnostic.
    #
    # + return - terminal failure context, or () when the Listener has not failed
    public function getLastError() returns ListenerError? {
        return self.terminalError;
    }

    // `forcedRecoveryPosition` bypasses the stored cursor entirely and starts
    // at that configured position instead; used only when Salesforce has just
    // rejected the stored cursor as invalid or expired.
    private function openTopicSubscription(wire:PubSubClient grpcClient, string topic, Service attachedService,
            ReplayPosition? forcedRecoveryPosition = ()) returns error? {
        string topicToken = check self.tokenManager.getAccessToken();
        map<string|string[]> topicHeaders = check metadataForIdentity(connectionIdentityFor(self.config.connection), topicToken);
        wire:TopicInfo|error topicResult = grpcClient->GetTopic({content: {topic_name: topic}, headers: topicHeaders});
        if topicResult is error && grpcStatusNameOf(topicResult) == "UNAUTHENTICATED" {
            check self.tokenManager.invalidateAccessToken();
            string refreshedTopicToken = check self.tokenManager.getAccessToken();
            map<string|string[]> refreshedTopicHeaders = check metadataForIdentity(
                connectionIdentityFor(self.config.connection), refreshedTopicToken);
            topicResult = grpcClient->GetTopic({content: {topic_name: topic}, headers: refreshedTopicHeaders});
        }
        wire:TopicInfo topicInfo = check topicResult;
        if !topicInfo.can_subscribe {
            return error("topic does not support subscribing");
        }
        SubscriptionConfig resolvedConfig = subscriptionConfigFor(self.config, attachedService);
        ReplayKey replayKey = {
            tenantId: self.config.connection.tenantId,
            topic,
            subscriptionName: self.config.logicalSubscriptionName
        };
        FlowController flow = check new (resolvedConfig.bufferSize);
        int initialCredit = check flow.initialRequest();
        // A dedicated channel for the Subscribe call itself: it must tolerate
        // sitting idle between events for arbitrarily long stretches (see
        // grpcConfigForListener), which is the wrong timeout for GetTopic
        // above -- sharing one client between both would let a transient
        // GetTopic connection issue hang for as long as the stream is allowed
        // to idle, instead of failing fast and letting the caller retry.
        wire:PubSubClient streamGrpcClient = check new (self.config.connection.endpoint, grpcConfigForListener(self.config.connection));
        string streamToken = check self.tokenManager.getAccessToken();
        map<string|string[]> streamHeaders = check metadataForIdentity(connectionIdentityFor(self.config.connection), streamToken);
        wire:SubscribeStreamingClient|error streamResult = streamGrpcClient->SubscribeContext(streamHeaders);
        if streamResult is error && grpcStatusNameOf(streamResult) == "UNAUTHENTICATED" {
            check self.tokenManager.invalidateAccessToken();
            string refreshedStreamToken = check self.tokenManager.getAccessToken();
            map<string|string[]> refreshedStreamHeaders = check metadataForIdentity(
                connectionIdentityFor(self.config.connection), refreshedStreamToken);
            streamResult = streamGrpcClient->SubscribeContext(refreshedStreamHeaders);
        }
        wire:SubscribeStreamingClient streamClient = check streamResult;
        wire:FetchRequest fetchRequest;
        if forcedRecoveryPosition is ReplayPosition {
            fetchRequest = check recoveryFetchRequest(topic, forcedRecoveryPosition, initialCredit);
        } else {
            byte[]? replayId = check self.config.replayStore.load(replayKey);
            fetchRequest = check initialFetchRequest(topic, replayId, resolvedConfig.initialReplay, initialCredit);
        }
        check streamClient->sendFetchRequest(fetchRequest);
        self.subscriptions[topic] = {streamClient, flow, gate: new, replayKey, config: resolvedConfig, attachedService};
    }

    // Recreates only a closed topic stream. Each reconnect reloads the latest
    // durable replay cursor and obtains new authentication metadata.
    private function consumeTopic(string topic) returns error? {
        error? result = self.consumeTopicUntilTerminal(topic);
        if result is error && self.started {
            error? cleanupError = self.terminateAfterFailure(topic, result);
            if cleanupError is error {
                return cleanupError;
            }
        }
        return result;
    }

    private function consumeTopicUntilTerminal(string topic) returns error? {
        int retryNumber = 0;
        while self.started {
            error? streamError = self.consumeOpenTopic(topic);
            if !self.started {
                return ();
            }
            error failure = streamError is error ? streamError : error("Subscribe stream closed");
            // A failure to set up the replacement (new channel, or the new
            // stream itself) is folded back into `failure` and re-classified
            // on the next spin of this inner loop, so it consumes the same
            // reconnect budget as a stream-read failure instead of aborting
            // the topic on the first attempt made during a still-ongoing
            // outage.
            while true {
                ActiveSubscription? previous = self.subscriptions[topic];
                boolean invalidReplay = isInvalidReplayError(failure);
                if previous !is ActiveSubscription || (!invalidReplay && !isReconnectableStreamError(failure)) {
                    return failure;
                }
                retryNumber += 1;
                if !canRetry(previous.config.reconnectRetry, retryNumber) {
                    return failure;
                }
                runtime:sleep(check retryDelay(previous.config.reconnectRetry, retryNumber));
                wire:SubscribeStreamingClient oldStream = previous.streamClient;
                error? closeError = oldStream->complete();
                if closeError is error {
                    // The stream is already unusable; opening its replacement is safe.
                }
                Service? attachedService = self.services[topic];
                if attachedService !is Service {
                    return error("listener subscription state is unavailable");
                }
                ReplayPosition? recoveryPosition = invalidReplay ? previous.config.expiredReplayRecovery : ();
                error? setupError = self.reopenTopicSubscription(topic, attachedService, failure, recoveryPosition);
                if setupError is error {
                    failure = setupError;
                    continue;
                }
                break;
            }
        }
    }

    // Rebuilds the channel (only when `causeOfReconnect` was channel-level)
    // and reopens the topic's stream. Returns any failure from either step
    // instead of `check`-ing it, so the caller can run it back through the
    // reconnect budget rather than treat the first setup failure as terminal.
    private function reopenTopicSubscription(string topic, Service attachedService, error causeOfReconnect,
            ReplayPosition? recoveryPosition) returns error? {
        wire:PubSubClient currentClient;
        if isChannelLevelFailure(causeOfReconnect) {
            wire:PubSubClient|error newClient = new (self.config.connection.endpoint,
                grpcConfigFor(self.config.connection));
            if newClient is error {
                return newClient;
            }
            currentClient = newClient;
            self.pubsubClient = currentClient;
        } else {
            wire:PubSubClient? existingClient = self.pubsubClient;
            if existingClient !is wire:PubSubClient {
                return error("listener subscription state is unavailable");
            }
            currentClient = existingClient;
        }
        return self.openTopicSubscription(currentClient, topic, attachedService, recoveryPosition);
    }

    // Stops every stream after a fatal subscription error. The error is
    // returned from the background worker for application supervision while
    // uncheckpointed events remain replayable from their saved cursor.
    private function terminateAfterFailure(string topic, error cause) returns error? {
        self.started = false;
        ListenerError terminalError = listenerErrorFor("Subscribe", topic, cause, grpcStatusNameOf(cause), (), ());
        self.terminalError = terminalError;
        error? firstError = ();
        foreach var [_, attachedService] in self.services.entries() {
            if attachedService is ErrorAwareService {
                error? notifyError = new ServiceErrorNotifier(attachedService).notify(terminalError);
                if notifyError is error && firstError is () {
                    firstError = notifyError;
                }
            }
        }
        foreach var [_, subscription] in self.subscriptions.entries() {
            wire:SubscribeStreamingClient streamClient = subscription.streamClient;
            error? closeError = streamClient->complete();
            if closeError is error && firstError is () {
                firstError = closeError;
            }
        }
        self.subscriptions = {};
        self.pubsubClient = ();
        if firstError is error {
            return firstError;
        }
    }

    private function consumeOpenTopic(string topic) returns error? {
        ActiveSubscription? current = self.subscriptions[topic];
        if current !is ActiveSubscription {
            return ();
        }
        wire:PubSubClient? currentClient = self.pubsubClient;
        if currentClient !is wire:PubSubClient {
            return error("listener transport is unavailable");
        }
        GrpcSchemaLoader schemaLoader = new (currentClient, connectionIdentityFor(self.config.connection), self.tokenManager);
        ServiceEventHandler handler = new (current.attachedService);
        wire:SubscribeStreamingClient streamClient = current.streamClient;
        while self.started {
            wire:FetchResponse? response = check streamClient->receiveFetchResponse();
            if response is () {
                return ();
            }
            if response.events.length() == 0 {
                if response.latest_replay_id.length() > 0 && current.gate.canCheckpointKeepalive() {
                    check self.config.replayStore.save(current.replayKey, response.latest_replay_id);
                }
                continue;
            }
            check current.flow.received(response.events.length());
            foreach wire:ConsumerEvent wireEvent in response.events {
                string schemaJson = check processSchemaFor(self.config.connection.tenantId, wireEvent.event.schema_id, schemaLoader);
                Event event = check eventFromConsumerEvent(topic, schemaJson, wireEvent);
                _ = check self.deliverWithRetries(event, current, handler);
                int replacementCredit = check current.flow.checkpointed();
                if replacementCredit > 0 {
                    check streamClient->sendFetchRequest({topic_name: topic, num_requested: replacementCredit});
                }
            }
        }
    }

    private function deliverWithRetries(Event event, ActiveSubscription current, EventHandler handler) returns Event|error {
        Event|error outcome = deliverDecodedEvent(event, current.replayKey, self.config.replayStore, handler, current.gate, true);
        int retryNumber = 0;
        while outcome is error {
            retryNumber += 1;
            if !canRetry(current.config.handlerRetry, retryNumber) {
                return outcome;
            }
            runtime:sleep(check retryDelay(current.config.handlerRetry, retryNumber));
            check current.gate.retryHandler();
            outcome = retryConsumerEvent(event, current.replayKey, self.config.replayStore, handler, current.gate);
        }
        return outcome;
    }
}
