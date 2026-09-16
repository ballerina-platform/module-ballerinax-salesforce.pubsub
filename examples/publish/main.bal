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

public function main() returns error? {
    pubsub:Publisher orders = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: accessToken},
            instanceUrl,
            tenantId
        },
        topic: "/event/Order_Notification__e"
    });

    pubsub:PublishResult[] results = check orders->publish([
        {payload: {
            "CreatedDate": 0,
            "CreatedById": "",
            "Order_Id__c": "ORDER-1042",
            "Status__c": "SHIPPED"
        }}
    ]);
    foreach pubsub:PublishResult result in results {
        if result.itemError is error {
            return result.itemError;
        }
    }
}
