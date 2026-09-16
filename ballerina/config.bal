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

# Resolves one attached service's effective delivery configuration and its
# `@ServiceConfig` annotation topic, if any. Presence of a @ServiceConfig
# annotation on the service replaces the listener's subscriptionConfig
# wholesale for that topic; absence falls back to the listener-wide default
# with no annotation topic.
#
# + defaultConfig - `ListenerConfig.subscriptionConfig`, used when the service
#   carries no @ServiceConfig annotation
# + attachedService - the service value passed to Listener.attach()
# + return - effective topic delivery configuration, and the annotation's
#   topic when the service carries a @ServiceConfig annotation that set one
public isolated function subscriptionConfigFor(SubscriptionConfig defaultConfig, Service attachedService)
        returns [SubscriptionConfig, string?] {
    typedesc<Service> serviceType = typeof attachedService;
    ServiceSubscriptionConfig? override = serviceType.@ServiceConfig;
    if override is ServiceSubscriptionConfig {
        SubscriptionConfig effective = {
            initialReplay: override.initialReplay,
            expiredReplayRecovery: override.expiredReplayRecovery,
            bufferSize: override.bufferSize,
            handlerRetry: override.handlerRetry,
            reconnectRetry: override.reconnectRetry
        };
        return [effective, override.topic];
    }
    return [defaultConfig, ()];
}

# Validates Listener configuration before a service is attached or transport is
# created. A per-service @ServiceConfig annotation override is validated
# separately in Listener.attach(), since it can only be discovered once a
# service value is presented there.
#
# + config - listener configuration to validate
# + return - an error when a connection or delivery setting is invalid
isolated function validateListenerConfig(ListenerConfig config) returns error? {
    check validateConnectionConfig(config.connection);
    if config.logicalSubscriptionName.length() == 0 {
        return error("logicalSubscriptionName must not be empty");
    }
    check validateSubscriptionConfig(config.subscriptionConfig);
}

isolated function validateSubscriptionConfig(SubscriptionConfig config) returns error? {
    if config.bufferSize <= 0 {
        return error("bufferSize must be greater than zero");
    }
    check validateRetryPolicy(config.handlerRetry);
    check validateRetryPolicy(config.reconnectRetry);
}
