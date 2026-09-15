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

# Identifies one durable cursor. A topic alone is not sufficient because a
# tenant can run multiple independent logical subscriptions for that topic.
public type ReplayKey record {| 
    # Salesforce tenant or org identifier.
    string tenantId;
    # Canonical Salesforce topic path.
    string topic;
    # Application-defined name that distinguishes subscriptions to the same topic.
    string subscriptionName;
|};

# Stores the exact opaque replay ID for a subscription.
public type ReplayStore distinct object {
    public isolated function load(ReplayKey key) returns byte[]|error?;
    public isolated function save(ReplayKey key, byte[] replayId) returns error?;
};

# Process-local replay storage for a single connector process. It is intended
# for V1 development and single-process deployments only.
public isolated class InMemoryReplayStore {
    *ReplayStore;
    private final map<byte[]> checkpoints = {};

    public isolated function load(ReplayKey key) returns byte[]|error? {
        lock {
            byte[]? replayId = self.checkpoints[key.toString()];
            if replayId is byte[] {
                return replayId.clone();
            }
        }
        return ();
    }

    public isolated function save(ReplayKey key, byte[] replayId) returns error? {
        lock {
            self.checkpoints[key.toString()] = replayId.clone();
        }
    }
}
