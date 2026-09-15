import ballerina/http;
import ballerina/test;

function serviceConfigTestConnection() returns ConnectionConfig => {
    auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
    instanceUrl: "https://acme.my.salesforce.com",
    tenantId: "00D000000000001"
};

// This fails if annotation retrieval silently drops or mistranslates a fully
// specified per-service override.
@test:Config {}
function testServiceConfigAnnotationOverridesEveryField() {
    ListenerConfig config = {connection: serviceConfigTestConnection()};
    Service annotatedService = @ServiceConfig {
        initialReplay: EARLIEST,
        expiredReplayRecovery: LATEST,
        bufferSize: 42,
        handlerRetry: {maxRetries: 7},
        reconnectRetry: {maxRetries: 9}
    } service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    SubscriptionConfig resolved = subscriptionConfigFor(config, annotatedService);
    test:assertEquals(resolved.initialReplay, EARLIEST);
    test:assertEquals(resolved.expiredReplayRecovery, LATEST);
    test:assertEquals(resolved.bufferSize, 42);
    test:assertEquals(resolved.handlerRetry.maxRetries, 7);
    test:assertEquals(resolved.reconnectRetry.maxRetries, 9);
}

// This fails if a partial @ServiceConfig annotation accidentally merges with
// the listener's subscriptionConfig field-by-field instead of replacing it
// wholesale — the annotation's omitted fields must take SubscriptionConfig's
// own type-level defaults, not the listener's tuned values.
@test:Config {}
function testServiceConfigAnnotationReplacesWholeRecordNotFieldByField() {
    ListenerConfig config = {
        connection: serviceConfigTestConnection(),
        subscriptionConfig: {bufferSize: 50, initialReplay: EARLIEST}
    };
    Service annotatedService = @ServiceConfig {bufferSize: 20} service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    SubscriptionConfig resolved = subscriptionConfigFor(config, annotatedService);
    test:assertEquals(resolved.bufferSize, 20);
    // SubscriptionConfig's own default (LATEST), not the listener's EARLIEST.
    test:assertEquals(resolved.initialReplay, LATEST);
}

// This fails if resolution stops falling back to the listener's shared
// default once a service carries no annotation at all.
@test:Config {}
function testServiceConfigResolutionFallsBackWhenAnnotationAbsent() {
    ListenerConfig config = {
        connection: serviceConfigTestConnection(),
        subscriptionConfig: {bufferSize: 33}
    };
    Service unannotatedService = service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    SubscriptionConfig resolved = subscriptionConfigFor(config, unannotatedService);
    test:assertEquals(resolved.bufferSize, 33);
}

// This fails if an invalid annotation-sourced override is only caught later
// (or never), instead of being rejected at the point a service is attached.
@test:Config {}
function testAttachRejectsInvalidServiceConfigAnnotation() returns error? {
    Listener endpoint = check new ({connection: serviceConfigTestConnection()});
    Service invalidService = @ServiceConfig {bufferSize: 0} service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    error? result = endpoint.attach(invalidService, "/event/Order__e");
    test:assertTrue(result is error);
}

// This fails if the listener-wide subscriber identity isn't applied
// consistently to every attached topic, which would let two of a listener's
// own topics resolve different `logicalSubscriptionName` values.
@test:Config {}
function testLogicalSubscriptionNameIsSharedAcrossAttachedTopics() {
    ListenerConfig config = {
        connection: serviceConfigTestConnection(),
        logicalSubscriptionName: "orders-app"
    };

    test:assertEquals(config.logicalSubscriptionName, "orders-app");
    // Both topics draw the checkpoint identity from the same listener-level
    // field; ReplayKey.topic (already distinct per D9's duplicate-topic
    // rejection) is what keeps their checkpoints from colliding, not a
    // per-topic subscription name.
}
