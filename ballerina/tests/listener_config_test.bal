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

import ballerina/test;
import ballerina/http;

function newOrderService() returns Service {
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
        }
    };
    return handler;
}

// This fails if a @ServiceConfig annotation is ignored, causing a service to
// use listener defaults instead of its own replay and flow-control policy.
@test:Config {}
function testListenerConfigAppliesServiceConfigAnnotation() {
    ListenerConfig config = {
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        },
        subscriptionConfig: {bufferSize: 10}
    };
    Service annotatedService = @ServiceConfig {bufferSize: 20} service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    SubscriptionConfig subscription = subscriptionConfigFor(config, annotatedService);
    test:assertEquals(subscription.bufferSize, 20);
}

// This fails if a Listener's default reconnect budget is small enough that a
// transient transport blip (UNAVAILABLE, a load balancer idle timeout, a
// brief network partition) permanently stops delivery instead of reconnecting
// through it -- a long-running Listener should only give up on a stream-level
// failure a caller explicitly configured it to give up on.
@test:Config {}
function testDefaultSubscriptionConfigHasEffectivelyUnboundedReconnectBudget() {
    SubscriptionConfig defaults = {};
    test:assertTrue(defaults.reconnectRetry.maxRetries > 1000000,
        "expected the default reconnect budget to be effectively unbounded, got: " +
        defaults.reconnectRetry.maxRetries.toString());
}

// Listener construction must reject an empty listener-wide subscription
// identity before it owns a transport or opens a Subscribe stream.
@test:Config {}
function testListenerConfigRejectsEmptyLogicalSubscriptionName() {
    ListenerConfig config = {
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        },
        logicalSubscriptionName: ""
    };

    test:assertTrue(validateListenerConfig(config) is error);
}

// Listener construction must reject invalid topic-scoped controls before it
// owns a transport or opens a Subscribe stream.
@test:Config {}
function testListenerConfigRejectsInvalidDeliverySettings() {
    ListenerConfig config = {
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        },
        subscriptionConfig: {bufferSize: 0}
    };

    test:assertTrue(validateListenerConfig(config) is error);
}

// Service registration only records the canonical topic locally. A duplicate
// must fail before it could create a second stream for the same checkpoint.
@test:Config {}
function testListenerRejectsDuplicateTopicAttachment() returns error? {
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        }
    });

    check endpoint.attach(newOrderService(), "/event/Order__e");
    error? duplicate = endpoint.attach(newOrderService(), "/event/Order__e");
    test:assertTrue(duplicate is error);
}

// Topic attachment is local and independent: separate canonical topics must
// coexist before start(), while only a duplicate of the same topic is rejected.
@test:Config {}
function testListenerAcceptsIndependentTopicAttachments() returns error? {
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        }
    });
    check endpoint.attach(newOrderService(), "/event/Order__e");
    check endpoint.attach(newOrderService(), "/data/AccountChangeEvent");
}

// This fails if attach() does not reassemble a compiler-supplied multi-segment
// service path (as an array of path segments, the shape a declarative
// `service /event/Order__e on listener` attachment actually supplies) into
// the exact same topic key a programmatic, single-string attach() would use
// for the equivalent topic.
@test:Config {}
function testListenerReassemblesMultiSegmentServicePathIntoTopic() returns error? {
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        }
    });

    check endpoint.attach(newOrderService(), ["event", "Order__e"]);
    error? duplicate = endpoint.attach(newOrderService(), "/event/Order__e");
    test:assertTrue(duplicate is error, "expected the array-path and string-path forms to resolve to the same topic key");
}

// A Listener has no terminal diagnostic until a worker actually fails; callers
// can safely poll this accessor during ordinary operation.
@test:Config {}
function testListenerHasNoTerminalErrorBeforeStart() returns error? {
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        }
    });
    test:assertEquals(endpoint.getLastError(), ());
}
