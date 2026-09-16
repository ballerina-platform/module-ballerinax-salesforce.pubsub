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
import ballerina/test;

// This fails if the public bearer-token OAuth configuration cannot be used for
// an authenticated Pub/Sub RPC while the shared provider seam is unreleased.
@test:Config {}
function testBearerConnectionProvidesAccessToken() returns error? {
    ConnectionConfig config = {
        auth: <http:BearerTokenConfig>{token: "test-token"},
        instanceUrl: "https://acme.my.salesforce.com",
        tenantId: "00D000000000001"
    };

    test:assertEquals(check accessTokenFor(config), "test-token");
}
