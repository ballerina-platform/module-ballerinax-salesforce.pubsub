import ballerina/test;
import ballerinax/salesforce.pubsub.internal as wire;

// This fails if an expired checkpoint recovery request retains CUSTOM or an
// old replay ID instead of beginning at the configured recovery position.
@test:Config {}
function testExpiredReplayRecoveryUsesConfiguredEarliestPosition() returns error? {
    wire:FetchRequest request = check recoveryFetchRequest("/event/Order__e", EARLIEST, 10);

    test:assertEquals(request.replay_preset, wire:EARLIEST);
    test:assertEquals(request.replay_id, []);
    test:assertEquals(request.num_requested, 10);
}
