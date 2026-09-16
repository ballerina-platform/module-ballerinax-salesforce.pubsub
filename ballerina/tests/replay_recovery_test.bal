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

// This fails if an expired checkpoint recovery request retains CUSTOM or an
// old replay ID instead of beginning at the configured recovery position.
@test:Config {}
function testExpiredReplayRecoveryUsesConfiguredEarliestPosition() returns error? {
    wire:FetchRequest request = check recoveryFetchRequest("/event/Order__e", EARLIEST, 10);

    test:assertEquals(request.replay_preset, wire:EARLIEST);
    test:assertEquals(request.replay_id, []);
    test:assertEquals(request.num_requested, 10);
}
