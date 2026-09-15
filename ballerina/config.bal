// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License, Version 2.0.

# Resolves one topic's delivery configuration. The service path remains the
# authoritative topic; an override only supplies delivery settings for it.
#
# + config - listener-wide configuration
# + topic - canonical attached service-path topic
# + return - effective topic delivery configuration
public isolated function subscriptionConfigFor(ListenerConfig config, string topic) returns SubscriptionConfig|error {
    if topic.length() == 0 {
        return error("topic must not be empty");
    }
    SubscriptionConfig? override = config.topicOverrides[topic];
    if override is SubscriptionConfig {
        return override;
    }
    return config.subscriptionDefaults;
}

# Validates Listener configuration before a service is attached or transport is
# created. Topic overrides may change only delivery settings, never identity.
#
# + config - listener configuration to validate
# + return - an error when a connection or delivery setting is invalid
isolated function validateListenerConfig(ListenerConfig config) returns error? {
    check validateConnectionConfig(config.connection);
    check validateSubscriptionConfig(config.subscriptionDefaults);
    foreach var [topic, subscription] in config.topicOverrides.entries() {
        if topic.length() == 0 {
            return error("topic override key must not be empty");
        }
        check validateSubscriptionConfig(subscription);
    }
}

isolated function validateSubscriptionConfig(SubscriptionConfig config) returns error? {
    if config.logicalSubscriptionName.length() == 0 {
        return error("logicalSubscriptionName must not be empty");
    }
    if config.bufferSize <= 0 {
        return error("bufferSize must be greater than zero");
    }
    check validateRetryPolicy(config.handlerRetry);
    check validateRetryPolicy(config.reconnectRetry);
}
