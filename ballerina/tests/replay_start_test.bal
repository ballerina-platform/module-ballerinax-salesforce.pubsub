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
import ballerinax/salesforce.pubsub.internal as wire;

// This fails if a stored checkpoint is ignored and a restarted subscription
// begins at LATEST instead of resuming with CUSTOM after that exact cursor.
@test:Config {}
function testReplayStartUsesCustomForStoredCursor() returns error? {
    wire:FetchRequest request = check initialFetchRequest("/event/Order__e", [1, 2], LATEST, 10);

    test:assertEquals(request.replay_preset, wire:CUSTOM);
    test:assertEquals(request.replay_id, [1, 2]);
}

// This fails if an empty store starts at an implicit older position instead of
// the configured default, which is LATEST for V1.
@test:Config {}
function testReplayStartUsesConfiguredPositionWithoutCursor() returns error? {
    wire:FetchRequest request = check initialFetchRequest("/event/Order__e", (), LATEST, 10);

    test:assertEquals(request.replay_preset, wire:LATEST);
    test:assertEquals(request.num_requested, 10);
}

// This fails if the connector can emit a zero-credit standard Subscribe
// request, which Salesforce rejects and which cannot keep the stream alive.
@test:Config {}
function testReplayStartRejectsZeroInitialCredit() {
    test:assertTrue(initialFetchRequest("/event/Order__e", (), LATEST, 0) is error);
}
