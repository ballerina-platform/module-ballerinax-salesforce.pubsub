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
    private map<Service> services = {};
    private map<ActiveSubscription> subscriptions = {};
    private map<future<error?>> workers = {};
    private wire:PubSubClient? pubsubClient = ();
    private boolean started = false;
    private ListenerError? terminalError = ();

    # Creates a Listener after validating configuration without opening a
    # network connection.
    public function init(ListenerConfig config) returns error? {
        check validateListenerConfig(config);
        self.config = config;
    }

    # Registers a service for its canonical Salesforce topic. The compiler
    # supplies the service path as `name` for `service "..." on listener`.
    #
    # + s - service with a remote `onEvent` method
    # + name - canonical Salesforce topic path
    # + return - an error for an invalid or duplicate topic
    public function attach(Service s, string[]|string? name = ()) returns error? {
        if name !is string || name.length() == 0 {
            return error("Listener service path must be one canonical topic string");
        }
        _ = check subscriptionConfigFor(self.config, name);
        if self.services.hasKey(name) {
            return error("a service is already attached for topic " + name);
        }
        self.services[name] = s;
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
        foreach var [_, waiterFuture] in waiters.entries() {
            error?|ShutdownTimedOut outcome = wait waiterFuture;
            if outcome is error && firstError is () {
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
        string accessToken = check accessTokenFor(self.config.connection);
        map<string|string[]> headers = check metadataFor(self.config.connection, accessToken);
        wire:TopicInfo topicInfo = check grpcClient->GetTopic({content: {topic_name: topic}, headers});
        if !topicInfo.can_subscribe {
            return error("topic does not support subscribing");
        }
        SubscriptionConfig subscriptionConfig = check subscriptionConfigFor(self.config, topic);
        ReplayKey replayKey = {
            tenantId: self.config.connection.tenantId,
            topic,
            subscriptionName: subscriptionConfig.logicalSubscriptionName
        };
        FlowController flow = check new (subscriptionConfig.bufferSize);
        int initialCredit = check flow.initialRequest();
        wire:SubscribeStreamingClient streamClient = check grpcClient->SubscribeContext(headers);
        wire:FetchRequest fetchRequest;
        if forcedRecoveryPosition is ReplayPosition {
            fetchRequest = check recoveryFetchRequest(topic, forcedRecoveryPosition, initialCredit);
        } else {
            byte[]? replayId = check self.config.replayStore.load(replayKey);
            fetchRequest = check initialFetchRequest(topic, replayId, subscriptionConfig.initialReplay, initialCredit);
        }
        check streamClient->sendFetchRequest(fetchRequest);
        self.subscriptions[topic] = {streamClient, flow, gate: new, replayKey, config: subscriptionConfig, attachedService};
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
            wire:PubSubClient currentClient;
            if isChannelLevelFailure(failure) {
                currentClient = check new (self.config.connection.endpoint, grpcConfigFor(self.config.connection));
                self.pubsubClient = currentClient;
            } else {
                wire:PubSubClient? existingClient = self.pubsubClient;
                if existingClient !is wire:PubSubClient {
                    return error("listener subscription state is unavailable");
                }
                currentClient = existingClient;
            }
            ReplayPosition? recoveryPosition = invalidReplay ? previous.config.expiredReplayRecovery : ();
            check self.openTopicSubscription(currentClient, topic, attachedService, recoveryPosition);
        }
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
        GrpcSchemaLoader schemaLoader = new (currentClient, self.config.connection);
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
