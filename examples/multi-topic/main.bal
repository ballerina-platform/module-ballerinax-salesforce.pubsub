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
import ballerina/io;
import ballerinax/salesforce.pubsub;

configurable string accessToken = ?;
configurable string instanceUrl = ?;
configurable string tenantId = ?;

listener pubsub:Listener events = check new ({
    connection: {
        auth: <http:BearerTokenConfig>{token: accessToken},
        instanceUrl,
        tenantId
    }
});

// Each declaratively attached topic gets its own independent stream, cursor,
// and sequential delivery; a slow or stuck handler on one topic never blocks
// the other's progress.
service /event/Order_Notification__e on events {
    remote function onEvent(pubsub:Event event) returns error? {
        // Convert event.payload to the application record type when needed.
    }

    // onError is optional per attached service. A terminal failure stops
    // every attached topic, not just this one, so this callback is a
    // listener-wide notification rather than a per-topic one.
    remote function onError(pubsub:ListenerError err) returns error? {
        io:println("Pub/Sub listener stopped after a terminal failure in ",
                err.operation, " (topic: ", err.topic ?: "unknown", ")");
    }
}

// A service is not required to define onError; onEvent alone is valid.
service /event/Shipment_Notification__e on events {
    remote function onEvent(pubsub:Event event) returns error? {
        // Convert event.payload to the application record type when needed.
    }
}
