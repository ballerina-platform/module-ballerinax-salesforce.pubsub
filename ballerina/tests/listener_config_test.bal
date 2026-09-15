import ballerina/test;
import ballerina/http;

function newOrderService() returns Service {
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
        }
    };
    return handler;
}

// This fails if a configured topic override is ignored, causing a service to
// use listener defaults instead of its own replay and flow-control policy.
@test:Config {}
function testListenerConfigAppliesTopicOverride() returns error? {
    ListenerConfig config = {
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        },
        subscriptionDefaults: {bufferSize: 10},
        topicOverrides: {
            "/event/Order__e": {logicalSubscriptionName: "orders", bufferSize: 20}
        }
    };

    SubscriptionConfig subscription = check subscriptionConfigFor(config, "/event/Order__e");
    test:assertEquals(subscription.logicalSubscriptionName, "orders");
    test:assertEquals(subscription.bufferSize, 20);
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
        subscriptionDefaults: {logicalSubscriptionName: "", bufferSize: 0}
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
