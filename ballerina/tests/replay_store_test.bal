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
