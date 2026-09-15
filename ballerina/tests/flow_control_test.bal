import ballerina/test;

// This fails if the connector asks Salesforce for zero events or exceeds its
// configured initial buffer when beginning a subscription.
@test:Config {}
function testFlowControllerRequestsBoundedInitialCredit() returns error? {
    FlowController controller = check new (10);

    test:assertEquals(check controller.initialRequest(), 10);
}

// This fails if a configured buffer above Salesforce's protocol maximum can
// make the initial FetchRequest exceed 100 events.
@test:Config {}
function testFlowControllerCapsInitialCreditAtProtocolMaximum() returns error? {
    FlowController controller = check new (101);

    test:assertEquals(check controller.initialRequest(), 100);
}

// This fails if a buffer exactly at the protocol maximum is miscounted as
// exceeding it (an off-by-one in the capping comparison).
@test:Config {}
function testFlowControllerRequestsExactProtocolMaximumUncapped() returns error? {
    FlowController controller = check new (100);

    test:assertEquals(check controller.initialRequest(), 100);
}

// This fails if a buffer just under the protocol maximum is capped anyway,
// which would request more capacity than the caller configured.
@test:Config {}
function testFlowControllerRequestsBufferSizeJustUnderProtocolMaximum() returns error? {
    FlowController controller = check new (99);

    test:assertEquals(check controller.initialRequest(), 99);
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

// This fails if checkpointing is ever accepted with no buffered event to
// account it against (for example a duplicate or out-of-order checkpoint
// call), which would silently desynchronize the credit count from reality.
@test:Config {}
function testFlowControllerRejectsCheckpointWithNoBufferedEvent() returns error? {
    FlowController controller = check new (2);
    _ = check controller.initialRequest();

    test:assertTrue(controller.checkpointed() is error);
}
