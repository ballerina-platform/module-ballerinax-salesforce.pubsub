import ballerina/test;

// This fails if a checkpoint is keyed only by topic, which would let one
// subscription resume from another subscription's cursor.
@test:Config {}
function testInMemoryReplayStoreIsolatesLogicalSubscriptions() returns error? {
    InMemoryReplayStore store = new;
    ReplayKey orders = {tenantId: "00D000000000001", topic: "/event/Order__e", subscriptionName: "orders"};
    ReplayKey audit = {tenantId: "00D000000000001", topic: "/event/Order__e", subscriptionName: "audit"};

    check store.save(orders, [1, 2, 3]);

    test:assertEquals(check store.load(orders), [1, 2, 3]);
    test:assertEquals(check store.load(audit), ());
}

// This fails if the store keeps a caller-owned mutable array instead of the
// exact replay bytes supplied when the checkpoint was saved.
@test:Config {}
function testInMemoryReplayStoreDefensivelyCopiesReplayIds() returns error? {
    InMemoryReplayStore store = new;
    ReplayKey key = {tenantId: "00D000000000001", topic: "/event/Order__e", subscriptionName: "orders"};
    byte[] replayId = [4, 5, 6];

    check store.save(key, replayId);
    replayId[0] = 99;

    test:assertEquals(check store.load(key), [4, 5, 6]);
}
