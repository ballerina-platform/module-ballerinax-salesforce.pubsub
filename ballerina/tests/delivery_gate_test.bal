import ballerina/test;

// The next event and replacement credit stay blocked after handler success
// until the corresponding replay ID is durably saved.
@test:Config {}
function testDeliveryGateRequiresCheckpointBeforeProgress() returns error? {
    DeliveryGate gate = new;
    check gate.beginEvent();
    check gate.handlerSucceeded();

    test:assertFalse(gate.canDeliverNext());
    test:assertFalse(gate.canReplenish());
    check gate.checkpointSaved();
    test:assertTrue(gate.canDeliverNext());
    test:assertTrue(gate.canReplenish());
}

// A handler or checkpoint failure preserves the unresolved event, so a later
// keepalive candidate cannot move the durable cursor beyond it.
@test:Config {}
function testDeliveryGateBlocksKeepaliveAfterFailure() returns error? {
    DeliveryGate gate = new;
    check gate.beginEvent();
    check gate.handlerSucceeded();
    gate.checkpointFailed();

    test:assertFalse(gate.canCheckpointKeepalive());
    test:assertTrue(gate.checkpointSaved() is error);
}

// Retrying the same failed event may resume its handler only; it must not open
// the gate to a later queued event.
@test:Config {}
function testDeliveryGateAllowsOnlyTheFailedEventToRetry() returns error? {
    DeliveryGate gate = new;
    check gate.beginEvent();
    gate.handlerFailed();

    check gate.retryHandler();
    test:assertFalse(gate.canDeliverNext());
    check gate.handlerSucceeded();
    check gate.checkpointSaved();
    test:assertTrue(gate.canDeliverNext());
}
