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

function attachTopicTestConnection() returns ConnectionConfig => {
    auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
    instanceUrl: "https://acme.my.salesforce.com",
    tenantId: "00D000000000001"
};

function attachTopicTestListener() returns Listener|error => new ({connection: attachTopicTestConnection()});

// This fails if a declarative attach (no compiler-supplied service path) ever
// succeeds without a service-level topic to key its subscription on.
@test:Config {}
function testAttachRejectsMissingTopicWhenNoPathOrAnnotation() returns error? {
    Listener endpoint = check attachTopicTestListener();
    Service handler = service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    error? result = endpoint.attach(handler);
    test:assertTrue(result is error);
}

// This fails if an empty @ServiceConfig.topic is ever accepted as a real
// Salesforce topic instead of being rejected before any network activity.
@test:Config {}
function testAttachRejectsEmptyAnnotationTopic() returns error? {
    Listener endpoint = check attachTopicTestListener();
    Service handler = @ServiceConfig {topic: ""} service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    error? result = endpoint.attach(handler);
    test:assertTrue(result is error);
}

// This fails if @ServiceConfig.topic alone (no compiler-supplied service
// path) is ignored during declarative attachment.
@test:Config {}
function testAttachUsesAnnotationTopicWhenNoServicePathGiven() returns error? {
    Listener endpoint = check attachTopicTestListener();
    Service handler = @ServiceConfig {topic: "/data/Donation__ChangeEvent"} service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    check endpoint.attach(handler);
    error? duplicate = endpoint.attach(handler, "/data/Donation__ChangeEvent");
    test:assertTrue(duplicate is error, "expected the annotation topic to have already claimed this topic");
}

// This fails if attach() lets a programmatic topic and a conflicting
// @ServiceConfig.topic both silently apply instead of being rejected before
// any network activity.
@test:Config {}
function testAttachRejectsConflictingProgrammaticAndAnnotationTopics() returns error? {
    Listener endpoint = check attachTopicTestListener();
    Service handler = @ServiceConfig {topic: "/data/Donation__ChangeEvent"} service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    error? result = endpoint.attach(handler, "/event/Different__e");
    test:assertTrue(result is error);
}

// This fails if attach() rejects a matching programmatic topic and
// annotation topic pair instead of treating an exact match as valid.
@test:Config {}
function testAttachAcceptsMatchingProgrammaticAndAnnotationTopics() returns error? {
    Listener endpoint = check attachTopicTestListener();
    Service handler = @ServiceConfig {topic: "/data/Donation__ChangeEvent"} service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    check endpoint.attach(handler, "/data/Donation__ChangeEvent");
}

// This fails if a bare `service on listener` ever resolves a literal "/" as
// though it were a real Salesforce topic rather than falling back to the
// annotation topic.
@test:Config {}
function testAttachTreatsRootSlashPathAsAbsent() returns error? {
    Listener endpoint = check attachTopicTestListener();
    Service handler = @ServiceConfig {topic: "/data/Donation__ChangeEvent"} service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    check endpoint.attach(handler, "/");
}
