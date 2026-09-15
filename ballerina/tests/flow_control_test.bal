import ballerina/test;

// This fails if the connector asks Salesforce for zero events or exceeds its
// configured initial buffer when beginning a subscription.
@test:Config {}
function testFlowControllerRequestsBoundedInitialCredit() returns error? {
    FlowController controller = check new (10);

    test:assertEquals(check controller.initialRequest(), 10);
}

// This fails if repeated liveness checks can grow the buffer beyond the one
// internal reserve slot permitted by the V1 contract.
@test:Config {}
function testFlowControllerUsesOnlyOneLivenessReserveSlot() returns error? {
    FlowController controller = check new (10);
    _ = check controller.initialRequest();
    check controller.received(10);

    test:assertEquals(check controller.requestLivenessReserve(), 1);
    test:assertEquals(check controller.requestLivenessReserve(), 0);
}

// This fails if a configured buffer above Salesforce's protocol maximum can
// make the initial FetchRequest exceed 100 events.
@test:Config {}
function testFlowControllerCapsInitialCreditAtProtocolMaximum() returns error? {
    FlowController controller = check new (101);

    test:assertEquals(check controller.initialRequest(), 100);
}

// This fails until a successfully checkpointed event frees exactly one normal
// slot and produces a positive replacement request. A handler return alone is
// deliberately insufficient because the replay save can still fail.
@test:Config {}
function testFlowControllerReplenishesOnlyAfterCheckpoint() returns error? {
    FlowController controller = check new (2);
    _ = check controller.initialRequest();
    check controller.received(2);

    test:assertEquals(check controller.checkpointed(), 1);
    test:assertEquals(check controller.checkpointed(), 1);
}

// This fails until consuming the liveness-reserve event releases that reserve
// without issuing another credit. The normal buffer remains full after the
// reserve event is durable.
@test:Config {}
function testFlowControllerDoesNotReplaceCheckpointedReserveEvent() returns error? {
    FlowController controller = check new (2);
    _ = check controller.initialRequest();
    check controller.received(2);
    _ = check controller.requestLivenessReserve();
    check controller.received(1);

    test:assertEquals(check controller.checkpointed(true), 0);
    test:assertEquals(check controller.requestLivenessReserve(), 1);
}
