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

import ballerina/http;
import ballerinax/salesforce.pubsub;

configurable string accessToken = ?;
configurable string instanceUrl = ?;
configurable string tenantId = ?;

listener pubsub:Listener donationEvents = check new ({
    connection: {
        auth: <http:BearerTokenConfig>{token: accessToken},
        instanceUrl,
        tenantId
    }
});

// The topic is a compile-time literal: point this at a differently named
// object/channel by editing it directly in source. A deployment that needs
// its topic chosen at runtime should use programmatic attach() instead.
@pubsub:ServiceConfig {
    topic: "/data/Donation__ChangeEvent"
}
service on donationEvents {
    remote function onEvent(pubsub:Event event) returns error? {
        // For /data/* topics, payload contains {changedData, metadata}.
        pubsub:Payload changedData = check event.payload["changedData"].ensureType();
        pubsub:Payload metadata = check event.payload["metadata"].ensureType();
        _ = changedData;
        _ = metadata;
    }
}
