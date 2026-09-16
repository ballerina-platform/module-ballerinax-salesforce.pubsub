# Declarative Pub/Sub Listener Services Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `pubsub:Listener` usable as a declarative Ballerina listener (`service on listener { ... }` with `@pubsub:ServiceConfig { topic: ... }` supplying the topic) while keeping the existing programmatic `attach()` API fully source-compatible, and make `Listener` an `isolated class`.

**Architecture:** Add an optional `topic` field to the existing `SubscriptionConfig` record (already reused as-is for `@pubsub:ServiceConfig`). Split `subscriptionConfigFor()` into a tuple return `[SubscriptionConfig, string?]` so callers get delivery settings and the annotation topic separately. Rewrite `Listener.attach()`'s topic resolution to combine the compiler-supplied service path with the annotation topic (exact match required if both given, error if neither). Convert `Listener` to `public isolated class`, protecting all mutable instance state with `lock` blocks (mirroring the existing `PubSubTokenManager`/`FlowController`/`DeliveryGate` isolation patterns already in this codebase), which requires widening `ReplayStore`'s abstract type to `isolated object`.

**Tech Stack:** Ballerina (jballerina.java, distribution per `ballerina/Ballerina.toml`), `bal build` / `bal test`.

**Spec:** The user-supplied plan in this conversation ("Declarative Pub/Sub Listener Services") — reproduced in full at the top of this session; no separate file. Key excerpts are inlined into each task below.

## Global Constraints

- `pubsub:Listener` changes from `public class` to `public isolated class`; all its methods must type-check as isolated.
- `SubscriptionConfig` gains `string? topic = ();`. All existing fields and their defaults are unchanged — every existing record literal and field access must keep compiling unchanged.
- Declarative attach (no compiler-supplied service path) requires a non-empty `@ServiceConfig.topic`; a missing/empty one is a local (pre-network) error.
- Programmatic `attach(service, topic)` keeps working with no annotation topic.
- When both a compiler-supplied path and an annotation topic are present, they must match exactly, else `attach()` errors before any network activity.
- A literal `"/"` service path (in case the compiler ever supplies a default root path for a bare `service on listener`) must never be treated as a real Salesforce topic — treat it the same as "no path supplied".
- Duplicate-topic rejection, per-topic `SubscriptionConfig` resolution, `ReplayKey`/replay-state keying, and terminal-diagnostic topic reporting must behave exactly as today.
- Never hand-edit `ballerina/Dependencies.toml` or `examples/*/Dependencies.toml` — let `bal build` regenerate them.
- Do not add explanatory comments beyond a factual one-liner for non-obvious WHY; match the terse doc-comment style already used throughout this module (see existing `#` doc comments in `client.bal`/`types.bal`).

---

## File Structure

- Modify `ballerina/types.bal`: add `topic` field to `SubscriptionConfig`; doc-comment update.
- Modify `ballerina/config.bal`: change `subscriptionConfigFor` to return `[SubscriptionConfig, string?]`.
- Modify `ballerina/client.bal`: convert `Listener` to `public isolated class`; rewrite `attach()`'s topic resolution; add a `resolveAttachTopic` helper; wrap mutable-field access in `lock` blocks.
- Modify `ballerina/replay_store.bal`: widen `ReplayStore` object type to `isolated object` (no behavior change; unlocks storing it directly in an isolated `Listener`).
- Modify `ballerina/tests/service_config_annotation_test.bal`, `ballerina/tests/listener_config_test.bal`: adapt to the new `subscriptionConfigFor` tuple return; add annotation-topic assertions.
- Modify `ballerina/tests/local_grpc_fixture_test.bal`: add declarative-attachment end-to-end tests (single topic, two independent topics) reusing the existing fixture listener/topics.
- Create `ballerina/tests/attach_topic_resolution_test.bal`: local (no-network) tests for missing/empty/duplicate/conflicting topic resolution.
- Modify `examples/listen/main.bal`, `examples/cdc/main.bal`, `examples/multi-topic/main.bal`: convert to the declarative `service on` form.
- Create `examples/donation-cdc/Ballerina.toml`, `examples/donation-cdc/main.bal` (the directory currently has only a stray untracked `Dependencies.toml` — there is no existing Donation CDC project in this repo to "update"; it must be created).

---

## Task 1: Extend `SubscriptionConfig` with an optional annotation topic

**Files:**
- Modify: `ballerina/types.bal:82-97` (`SubscriptionConfig` record and its doc comment)
- Test: `ballerina/tests/public_api_test.bal` (existing `testSubscriptionConfigHasSafeV1Defaults`)

**Interfaces:**
- Produces: `SubscriptionConfig.topic` — `string?`, default `()`. Consumed by Task 2 (`subscriptionConfigFor`) and Task 3 (`Listener.attach`).

- [ ] **Step 1: Update the doc comment and add the field**

Replace the `SubscriptionConfig` type in `ballerina/types.bal`:

```ballerina
# Topic-scoped sequential delivery configuration. Attach as `@pubsub:ServiceConfig`
# on a service declaration to override `ListenerConfig.subscriptionConfig` for
# that topic. An override replaces the whole record: any field the annotation
# omits takes this type's own default, not `subscriptionConfig`'s value.
public type SubscriptionConfig record {|
    # Position used when no checkpoint exists.
    ReplayPosition initialReplay = LATEST;
    # Position used when Salesforce rejects an expired checkpoint.
    ReplayPosition expiredReplayRecovery = EARLIEST;
    # Normal maximum number of events awaiting sequential processing.
    int bufferSize = 10;
    # Retry configuration for handler failures.
    RetryPolicy handlerRetry = {};
    # Retry configuration for reconnectable stream failures.
    RetryPolicy reconnectRetry = {};
    # Canonical Salesforce topic for a declarative `service on listener`
    # attachment. Required when `@pubsub:ServiceConfig` is the only topic
    # source; ignored when this record is used as `ListenerConfig.subscriptionConfig`.
    string? topic = ();
|};
```

- [ ] **Step 2: Confirm the existing default-values test still passes**

Run: `cd ballerina && bal test --tests testSubscriptionConfigHasSafeV1Defaults`
Expected: PASS (the test only asserts `initialReplay`/`expiredReplayRecovery`/`bufferSize`; adding `topic` does not affect it).

- [ ] **Step 3: Commit**

```bash
git add ballerina/types.bal
git commit -m "Add optional topic field to SubscriptionConfig for declarative attachment"
```

---

## Task 2: Split `subscriptionConfigFor` into delivery config + annotation topic

**Files:**
- Modify: `ballerina/config.bal:17-29` (`subscriptionConfigFor`)
- Modify: `ballerina/tests/service_config_annotation_test.bal` (all 4 call sites)
- Modify: `ballerina/tests/listener_config_test.bal:45` (`testListenerConfigAppliesServiceConfigAnnotation`)

**Interfaces:**
- Consumes: `SubscriptionConfig.topic` from Task 1.
- Produces: `subscriptionConfigFor(ListenerConfig, Service) returns [SubscriptionConfig, string?]` — first element is the effective delivery config (unchanged resolution rules), second is the annotation's `topic` (or `()` when no annotation, or the annotation didn't set one). Consumed by Task 3's `attach()`.

- [ ] **Step 1: Update the failing tests first (tuple destructuring)**

In `ballerina/tests/service_config_annotation_test.bal`, change every call site from:
```ballerina
SubscriptionConfig resolved = subscriptionConfigFor(config, annotatedService);
```
to:
```ballerina
[SubscriptionConfig resolved, string? resolvedTopic] = subscriptionConfigFor(config, annotatedService);
```
(for `testServiceConfigResolutionFallsBackWhenAnnotationAbsent`, name the unused topic `_`: `[SubscriptionConfig resolved, _] = subscriptionConfigFor(config, unannotatedService);`)

Add a new assertion to `testServiceConfigAnnotationOverridesEveryField` proving topic comes through:
```ballerina
@test:Config {}
function testServiceConfigAnnotationOverridesEveryField() {
    ListenerConfig config = {connection: serviceConfigTestConnection()};
    Service annotatedService = @ServiceConfig {
        initialReplay: EARLIEST,
        expiredReplayRecovery: LATEST,
        bufferSize: 42,
        handlerRetry: {maxRetries: 7},
        reconnectRetry: {maxRetries: 9},
        topic: "/data/Donation__ChangeEvent"
    } service object {
        remote function onEvent(Event event) returns error? {
        }
    };

    [SubscriptionConfig resolved, string? resolvedTopic] = subscriptionConfigFor(config, annotatedService);
    test:assertEquals(resolved.initialReplay, EARLIEST);
    test:assertEquals(resolved.expiredReplayRecovery, LATEST);
    test:assertEquals(resolved.bufferSize, 42);
    test:assertEquals(resolved.handlerRetry.maxRetries, 7);
    test:assertEquals(resolved.reconnectRetry.maxRetries, 9);
    test:assertEquals(resolvedTopic, "/data/Donation__ChangeEvent");
}
```

In `ballerina/tests/listener_config_test.bal`, change:
```ballerina
SubscriptionConfig subscription = subscriptionConfigFor(config, annotatedService);
```
to:
```ballerina
[SubscriptionConfig subscription, _] = subscriptionConfigFor(config, annotatedService);
```

- [ ] **Step 2: Run tests to confirm they fail to compile against the old signature**

Run: `cd ballerina && bal test`
Expected: compile error — too many/mismatched binding targets for `subscriptionConfigFor`'s current single-value return.

- [ ] **Step 3: Change the implementation**

Replace in `ballerina/config.bal`:
```ballerina
# Resolves one attached service's effective delivery configuration and its
# `@ServiceConfig` annotation topic, if any. Presence of a @ServiceConfig
# annotation on the service replaces the listener's subscriptionConfig
# wholesale for that topic; absence falls back to subscriptionConfig with no
# annotation topic.
#
# + config - listener-wide configuration
# + attachedService - the service value passed to Listener.attach()
# + return - effective topic delivery configuration, and the annotation's
#   topic when the service carries a @ServiceConfig annotation that set one
public isolated function subscriptionConfigFor(ListenerConfig config, Service attachedService)
        returns [SubscriptionConfig, string?] {
    typedesc<Service> serviceType = typeof attachedService;
    SubscriptionConfig? override = serviceType.@ServiceConfig;
    if override is SubscriptionConfig {
        return [override, override.topic];
    }
    return [config.subscriptionConfig, ()];
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cd ballerina && bal test`
Expected: PASS for all tests in `service_config_annotation_test.bal` and `listener_config_test.bal`. (`client.bal`'s two call sites will not yet compile — that's fixed in Task 3, done in the same build/test cycle since Ballerina compiles the whole module together; expect the overall `bal test` to fail at this step specifically due to `client.bal`, not due to the test files themselves. Proceed to Task 3 before re-running the full suite.)

- [ ] **Step 5: Commit** (fold into Task 3's commit, since `client.bal` must change in the same compile-passing unit — see Task 3 Step 6)

---

## Task 3: Rewrite `Listener.attach()`'s topic resolution

**Files:**
- Modify: `ballerina/client.bal:103-121` (`attach`)
- Test: `ballerina/tests/listener_config_test.bal` (existing duplicate/independent-topic tests must keep passing), `ballerina/tests/service_config_annotation_test.bal` (`testAttachRejectsInvalidServiceConfigAnnotation`)
- Create: `ballerina/tests/attach_topic_resolution_test.bal`

**Interfaces:**
- Consumes: `subscriptionConfigFor` tuple from Task 2.
- Produces: `Listener.attach(Service, string[]|string?) returns error?` — same signature, new resolution semantics: compiler-supplied path (when present, non-empty, and not `"/"`) and `@ServiceConfig.topic` (when present and non-empty) must agree if both given; either alone is used; neither is a local error.

- [ ] **Step 1: Write the new local (no-network) resolution tests first**

Create `ballerina/tests/attach_topic_resolution_test.bal`:

```ballerina
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
```

- [ ] **Step 2: Run the new tests to confirm they fail against the current `attach()`**

Run: `cd ballerina && bal test --tests 'testAttach*'`
Expected: FAIL (current `attach()` unconditionally rejects a `()`/empty/`"/"` path regardless of any annotation, and has no conflict-matching logic).

- [ ] **Step 3: Implement `resolveAttachTopic` and rewrite `attach()`**

In `ballerina/client.bal`, add this function near `attach` (module-level, above the `Listener` class or just before `attach`):

```ballerina
// A literal "/" is grammar for a pathless `service on listener` declaration
// on some Ballerina versions, never a real Salesforce topic, so it is
// normalized away exactly like an absent or empty path.
isolated function normalizedAttachTopic(string? candidate) returns string? =>
    candidate is string && candidate.length() > 0 && candidate != "/" ? candidate : ();

// Combines the compiler-supplied service path with the service's
// `@ServiceConfig.topic`, if any. Exactly one non-empty source is required;
// two non-empty sources must agree.
isolated function resolveAttachTopic(string? servicePath, string? annotationTopic) returns string|error {
    string? path = normalizedAttachTopic(servicePath);
    string? annotated = normalizedAttachTopic(annotationTopic);
    if path is string && annotated is string {
        if path != annotated {
            return error("attach() topic '" + path + "' conflicts with @ServiceConfig topic '" + annotated + "'");
        }
        return path;
    }
    if path is string {
        return path;
    }
    if annotated is string {
        return annotated;
    }
    return error("a Listener service needs one canonical topic: pass it to attach(), " +
            "or annotate the service with @ServiceConfig { topic: \"...\" } for declarative attachment");
}
```

Replace `attach()`'s body in the `Listener` class:

```ballerina
    # Registers a service for its canonical Salesforce topic. The compiler
    # supplies the service path as `name` for `service "..." on listener`; a
    # declarative `service on listener` (no path) resolves the topic from the
    # service's `@ServiceConfig.topic` instead.
    #
    # + s - service with a remote `onEvent` method
    # + name - canonical Salesforce topic path, when supplied programmatically
    # + return - an error for a missing, empty, or conflicting topic, a
    #   duplicate topic, or an invalid effective subscription configuration,
    #   including one supplied by a @ServiceConfig annotation
    public isolated function attach(Service s, string[]|string? name = ()) returns error? {
        if name is string[] {
            return error("Listener service path must be one canonical topic string");
        }
        [SubscriptionConfig, string?] [resolvedConfig, annotationTopic] = subscriptionConfigFor(self.config, s);
        check validateSubscriptionConfig(resolvedConfig);
        string topic = check resolveAttachTopic(name, annotationTopic);
        lock {
            if self.services.hasKey(topic) {
                return error("a service is already attached for topic " + topic);
            }
            self.services[topic] = s;
        }
    }
```

(The `lock` block here anticipates Task 4's isolation conversion; it is harmless — and required — even before `Listener` is declared `isolated`, since `self.services` remains a plain mutable map field.)

- [ ] **Step 4: Run the new tests to confirm they pass**

Run: `cd ballerina && bal test --tests 'testAttach*'`
Expected: PASS.

- [ ] **Step 5: Run the full suite to confirm no regressions**

Run: `cd ballerina && bal test`
Expected: all previously-passing tests still pass (in particular `testListenerRejectsDuplicateTopicAttachment`, `testListenerAcceptsIndependentTopicAttachments`, `testAttachRejectsInvalidServiceConfigAnnotation`).

- [ ] **Step 6: Commit** (includes Task 2's `config.bal`/test changes, since they only compile together with this task's `client.bal` change)

```bash
git add ballerina/types.bal ballerina/config.bal ballerina/client.bal \
    ballerina/tests/service_config_annotation_test.bal ballerina/tests/listener_config_test.bal \
    ballerina/tests/attach_topic_resolution_test.bal
git commit -m "Resolve attach() topic from compiler-supplied path and/or @ServiceConfig.topic"
```

---

## Task 4: Convert `Listener` to `public isolated class`

**Files:**
- Modify: `ballerina/replay_store.bal:29-32` (`ReplayStore` abstract type)
- Modify: `ballerina/client.bal` (whole `Listener` class: field declarations and every method)
- Test: full existing suite, especially `ballerina/tests/local_grpc_fixture_test.bal` (uses `Listener` heavily), `ballerina/tests/sandbox_test.bal` (uses `runtime:registerListener`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `Listener` remains structurally identical from callers' perspective (`init`, `attach`, `'start`, `gracefulStop`, `immediateStop`, `getLastError` all keep their signatures) but the class itself, and all its methods, are `isolated`.

- [ ] **Step 1: Widen `ReplayStore` to an isolated object type**

In `ballerina/replay_store.bal`, change:
```ballerina
public type ReplayStore distinct object {
    public isolated function load(ReplayKey key) returns byte[]|error?;
    public isolated function save(ReplayKey key, byte[] replayId) returns error?;
};
```
to:
```ballerina
public type ReplayStore distinct isolated object {
    public isolated function load(ReplayKey key) returns byte[]|error?;
    public isolated function save(ReplayKey key, byte[] replayId) returns error?;
};
```
`InMemoryReplayStore` already implements both methods as `isolated` with only a `final` isolated-safe field, so this is a non-breaking widening.

- [ ] **Step 2: Run the full suite to confirm this alone is a no-op**

Run: `cd ballerina && bal test`
Expected: all tests still pass (this step only changes a type descriptor, no behavior).

- [ ] **Step 3: Mark the class and every method isolated; lock-protect mutable fields**

In `ballerina/client.bal`, change the class declaration and field block:
```ballerina
public isolated class Listener {
    private final ListenerConfig config;
    private final PubSubTokenManager tokenManager;
    private map<Service> services = {};
    private map<ActiveSubscription> subscriptions = {};
    private map<future<error?>> workers = {};
    private wire:PubSubClient? pubsubClient = ();
    private boolean started = false;
    private ListenerError? terminalError = ();
```
`ListenerConfig` contains `ConnectionConfig.tokenStore: salesforce:TokenStore` (already an isolated object, matching `PubSubTokenManager`'s existing direct storage of `ConnectionConfig`-derived values) and, after Step 1, `ReplayStore` (now isolated) plus plain data fields — so a `final ListenerConfig` field is isolated-compatible as-is; only `services`, `subscriptions`, `workers`, `pubsubClient`, `started`, and `terminalError` need `lock` blocks around every access, since none of them are `final`.

Mark every public and private function `isolated function` (`init`, `attach` — already done in Task 3 —, `'start`, `gracefulStop`, `immediateStop`, `stopInternal`, `getLastError`, `openTopicSubscription`, `consumeTopic`, `consumeTopicUntilTerminal`, `terminateAfterFailure`, `consumeOpenTopic`, `deliverWithRetries`).

Wrap every read or write of `self.services`, `self.subscriptions`, `self.workers`, `self.pubsubClient`, `self.started`, `self.terminalError` in a `lock { ... }` block. Concretely:

- `'start()`: wrap the `self.started` check/read, and separately the loop that populates `self.subscriptions` via `openTopicSubscription` cannot itself be inside one giant lock (it calls out to network I/O, which is not allowed inside `lock`), so keep `self.services.entries()` read in its own short lock copying to a local array first, e.g.:
  ```ballerina
  public isolated function 'start() returns error? {
      lock {
          if self.started {
              return ();
          }
      }
      wire:PubSubClient grpcClient = check new (self.config.connection.endpoint, grpcConfigFor(self.config.connection));
      lock {
          self.pubsubClient = grpcClient;
      }
      [string, Service][] attachedServices = [];
      lock {
          foreach var [topic, attachedService] in self.services.entries() {
              attachedServices.push([topic, attachedService]);
          }
      }
      foreach var [topic, attachedService] in attachedServices {
          error? openError = self.openTopicSubscription(grpcClient, topic, attachedService);
          if openError is error {
              error? cleanupError = self.gracefulStop();
              if cleanupError is error {
                  return cleanupError;
              }
              return openError;
          }
      }
      lock {
          self.started = true;
      }
      string[] topics = [];
      lock {
          foreach string topic in self.subscriptions.keys() {
              topics.push(topic);
          }
      }
      map<future<error?>> newWorkers = {};
      foreach string topic in topics {
          newWorkers[topic] = start self.consumeTopic(topic);
      }
      lock {
          self.workers = newWorkers;
      }
  }
  ```
  (`Service` is a `service object {}` type; storing it in a plain, non-isolated local array/tuple outside a lock is fine as long as the array itself isn't a field — only object/class *fields* need isolation proof, not locals built from a lock-read snapshot. If the compiler still rejects this because `Service` isn't provably isolated, fall back to iterating and calling `openTopicSubscription` while still holding the lock only long enough to grab one `[topic, service]` pair per iteration via a `foreach` over a locally-copied `map<Service>` clone: `map<Service> servicesSnapshot; lock { servicesSnapshot = self.services.clone(); }` then `foreach var [topic, attachedService] in servicesSnapshot.entries() { ... }` outside the lock.)

- `gracefulStop`/`immediateStop`/`stopInternal(workerWaitTimeoutSeconds)`: wrap the `self.started = false;` write, the read of `self.subscriptions.entries()` (snapshot into a local map/array first the same way), the read/write of `self.workers`, and the final reset of `self.subscriptions = {}; self.workers = {}; self.pubsubClient = ();` in `lock` blocks. The `wait waiterFuture` calls must stay outside any `lock` (waiting inside a lock is not permitted).

- `getLastError()`:
  ```ballerina
  public isolated function getLastError() returns ListenerError? {
      lock {
          return self.terminalError;
      }
  }
  ```
  (`ListenerError` must itself be safely returnable from a lock — it's a plain record of strings and one `error`, which is fine; if the compiler complains about returning a non-isolated value out of `lock`, return `self.terminalError.cloneReadOnly()` typed `readonly & ListenerError?` instead, or clone the record fields explicitly.)

- `openTopicSubscription`, `consumeTopicUntilTerminal`, `terminateAfterFailure`, `consumeOpenTopic`, `deliverWithRetries`: wrap each direct read/write of `self.subscriptions[...]`, `self.services[...]`, `self.pubsubClient`, `self.terminalError` in its own short `lock` block, keeping all network/blocking calls (`grpcClient->...`, `streamClient->...`, `runtime:sleep`, `self.tokenManager.getAccessToken()`, `self.config.replayStore.load/save`) outside any `lock`.

This step is inherently iterative against the compiler: implement, then run `bal build` and fix each isolation diagnostic it reports (missing lock, non-isolated field type, disallowed call inside lock) one at a time rather than guessing every site up front.

- [ ] **Step 4: Build until clean**

Run: `cd ballerina && bal build`
Expected: eventually zero compile errors (isolation diagnostics resolved). Iterate Step 3 against each reported error.

- [ ] **Step 5: Run the full suite**

Run: `cd ballerina && bal test`
Expected: all tests pass, including every `Listener`-based test in `local_grpc_fixture_test.bal`, `listener_config_test.bal`, `service_config_annotation_test.bal`, `attach_topic_resolution_test.bal`, `listener_error_test.bal`.

- [ ] **Step 6: Commit**

```bash
git add ballerina/replay_store.bal ballerina/client.bal
git commit -m "Convert Listener to an isolated class"
```

---

## Task 5: Declarative end-to-end tests through the local TLS fixture

**Files:**
- Modify: `ballerina/tests/local_grpc_fixture_test.bal`

**Interfaces:**
- Consumes: `resolveAttachTopic`/`attach()` from Task 3, isolated `Listener` from Task 4, existing fixture helpers (`FIXTURE_PORT`, `FIXTURE_TOPIC`, `FIXTURE_MULTI_EVENT_TOPIC`, the fixture's TLS cert at `tests/resources/local-grpc.crt`) already used by `testListenerUsesLocalTlsGrpcFixture` (see that test at `ballerina/tests/local_grpc_fixture_test.bal:700-734` for the exact connection/config shape to reuse).
- Produces: two new `@test:Config` tests proving declarative attachment end-to-end.

- [ ] **Step 1: Write the failing tests**

Add to `ballerina/tests/local_grpc_fixture_test.bal` (near `testListenerUsesLocalTlsGrpcFixture`, reusing its exact connection block):

```ballerina
// This fails if a declarative `service on listener` (no compiler-supplied
// service path) cannot resolve its topic from @ServiceConfig, open its
// stream, decode an event, and checkpoint — proving the whole declarative
// path end-to-end, not just local topic resolution.
@test:Config {}
function testListenerAttachesDeclarativeServiceThroughLocalTlsGrpcFixture() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    @ServiceConfig {topic: FIXTURE_TOPIC}
    service object {
        remote function onEvent(Event event) returns error? {
            test:assertEquals(event.payload["Message__c"], "streamed");
        }
    } handler = service object {
        remote function onEvent(Event event) returns error? {
            test:assertEquals(event.payload["Message__c"], "streamed");
        }
    };
    check endpoint.attach(handler);
    check endpoint.'start();
    runtime:sleep(0.2);
    check endpoint.immediateStop();
    byte[]? replayId = check replayStore.load({
        tenantId: "00DFixture000001",
        topic: FIXTURE_TOPIC,
        subscriptionName: "default"
    });
    test:assertEquals(replayId, [7, 8, 9]);
}

// This fails if two declaratively-attached services on one Listener do not
// resolve to two fully independent topic subscriptions (separate streams,
// separate checkpoints).
@test:Config {}
function testListenerBindsTwoDeclarativeServicesToIndependentTopics() returns error? {
    InMemoryReplayStore replayStore = new;
    Listener endpoint = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "fixture-token"},
            instanceUrl: "https://fixture.my.salesforce.com",
            tenantId: "00DFixture000001",
            endpoint: "https://localhost:" + FIXTURE_PORT.toString(),
            grpcConfig: {secureSocket: {cert: "tests/resources/local-grpc.crt"}}
        },
        replayStore,
        subscriptionConfig: {handlerRetry: {maxRetries: 0}, reconnectRetry: {maxRetries: 0}}
    });
    Service firstHandler = @ServiceConfig {topic: FIXTURE_TOPIC} service object {
        remote function onEvent(Event event) returns error? {
        }
    };
    Service secondHandler = @ServiceConfig {topic: FIXTURE_MULTI_EVENT_TOPIC} service object {
        remote function onEvent(Event event) returns error? {
        }
    };
    check endpoint.attach(firstHandler);
    check endpoint.attach(secondHandler);
    check endpoint.'start();
    runtime:sleep(0.3);
    check endpoint.immediateStop();

    byte[]? firstReplay = check replayStore.load({
        tenantId: "00DFixture000001", topic: FIXTURE_TOPIC, subscriptionName: "default"
    });
    byte[]? secondReplay = check replayStore.load({
        tenantId: "00DFixture000001", topic: FIXTURE_MULTI_EVENT_TOPIC, subscriptionName: "default"
    });
    test:assertEquals(firstReplay, [7, 8, 9]);
    test:assertTrue(secondReplay is byte[], "expected the second declarative topic to have its own checkpoint");
}
```

(If the anonymous-service-variable-with-explicit-type form above doesn't parse cleanly for the first test, simplify `handler` to a plain `Service handler = @ServiceConfig {topic: FIXTURE_TOPIC} service object { remote function onEvent(Event event) returns error? { test:assertEquals(event.payload["Message__c"], "streamed"); } };` — drop the redundant explicit anonymous-service-typed declaration, which was accidentally duplicated above.)

- [ ] **Step 2: Run to confirm they fail before Tasks 1-4 land, pass after**

Run: `cd ballerina && bal test --tests 'testListenerAttachesDeclarativeServiceThroughLocalTlsGrpcFixture|testListenerBindsTwoDeclarativeServicesToIndependentTopics'`
Expected: PASS (assuming Tasks 1-4 are already committed; this task is written to run after them).

- [ ] **Step 3: Run the full suite**

Run: `cd ballerina && bal test`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add ballerina/tests/local_grpc_fixture_test.bal
git commit -m "Add declarative-attachment end-to-end fixture tests"
```

---

## Task 6: Verify the real `service on listener` declaration form compiles and resolves correctly

**Files:**
- Create (temporary, deleted at end of task): `ballerina/tests/declarative_syntax_smoke_test.bal`

**Interfaces:**
- Consumes: everything above.
- Produces: empirical confirmation of what the compiler actually passes as `name` for a bare `service on listener` declaration (`()` vs `"/"`), resolving the plan's stated assumption.

- [ ] **Step 1: Write a throwaway smoke test using the real declarative syntax**

```ballerina
import ballerina/http;
import ballerina/test;

@test:Config {}
function testDeclarativeServiceOnSyntaxAttachesViaAnnotationTopic() returns error? {
    Listener declListener = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: "ignored-in-test"},
            instanceUrl: "https://acme.my.salesforce.com",
            tenantId: "00D000000000001"
        }
    });
    @ServiceConfig {topic: "/data/Donation__ChangeEvent"}
    service on declListener {
        remote function onEvent(Event event) returns error? {
        }
    }
    // No error means the compiler-supplied path (whatever it was: (), "/",
    // or something else) combined with the annotation topic without
    // conflicting.
}
```

- [ ] **Step 2: Run it**

Run: `cd ballerina && bal test --tests testDeclarativeServiceOnSyntaxAttachesViaAnnotationTopic`
Expected: PASS. If it fails with a topic-conflict error, that proves the compiler supplies something other than `()`/`""`/`"/"` for a bare path — inspect the error message (it includes the literal conflicting path) and extend `normalizedAttachTopic` in `ballerina/client.bal` (Task 3) to also normalize away that literal value, then re-run.

- [ ] **Step 3: Delete the throwaway file once it passes (its coverage is superseded by Task 5's fixture tests and the assumption is now verified)**

```bash
rm ballerina/tests/declarative_syntax_smoke_test.bal
```

- [ ] **Step 4: Run the full suite once more**

Run: `cd ballerina && bal test`
Expected: all tests pass without the smoke test file.

- [ ] **Step 5: Commit only if `normalizedAttachTopic` changed** (otherwise nothing to commit for this task)

```bash
git add ballerina/client.bal
git commit -m "Normalize the compiler-supplied root path observed for a bare service-on declaration"
```

---

## Task 7: Convert `examples/listen`, `examples/cdc`, `examples/multi-topic` to declarative form

**Files:**
- Modify: `examples/listen/main.bal`
- Modify: `examples/cdc/main.bal`
- Modify: `examples/multi-topic/main.bal`

**Interfaces:**
- Consumes: the declarative syntax confirmed working in Task 6.
- Produces: no new public interfaces; example programs only.

- [ ] **Step 1: Rewrite `examples/listen/main.bal`**

```ballerina
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

listener pubsub:Listener events = check new ({
    connection: {
        auth: <http:BearerTokenConfig>{token: accessToken},
        instanceUrl,
        tenantId
    }
});

@pubsub:ServiceConfig {
    topic: "/event/Order_Notification__e"
}
service on events {
    remote function onEvent(pubsub:Event event) returns error? {
        // Convert event.payload to the application record type when needed.
    }
}
```

- [ ] **Step 2: Rewrite `examples/cdc/main.bal`** (same pattern, topic `/data/ChangeEvents`, keep the existing `changedData`/`metadata` extraction body).

- [ ] **Step 3: Rewrite `examples/multi-topic/main.bal`** using two `service on` declarations (one per topic) to demonstrate `@ServiceConfig.topic` binding two declarative services on one Listener to independent subscriptions — keep the existing `orders`/`shipments` comment content and the `onError` example on the `orders` service:

```ballerina
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
@pubsub:ServiceConfig {
    topic: "/event/Order_Notification__e"
}
service on events {
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
@pubsub:ServiceConfig {
    topic: "/event/Shipment_Notification__e"
}
service on events {
    remote function onEvent(pubsub:Event event) returns error? {
        // Convert event.payload to the application record type when needed.
    }
}
```

- [ ] **Step 4: Build each example**

Run: `cd examples/listen && bal build && cd ../cdc && bal build && cd ../multi-topic && bal build`
Expected: all three build cleanly (each regenerates its own `Dependencies.toml` — do not hand-edit those).

- [ ] **Step 5: Commit**

```bash
git add examples/listen/main.bal examples/cdc/main.bal examples/multi-topic/main.bal \
    examples/listen/Dependencies.toml examples/cdc/Dependencies.toml examples/multi-topic/Dependencies.toml
git commit -m "Convert listen, cdc, and multi-topic examples to declarative service form"
```

---

## Task 8: Create the Donation CDC example project (declarative form)

**Files:**
- Create: `examples/donation-cdc/Ballerina.toml`
- Create: `examples/donation-cdc/main.bal`
- Regenerate: `examples/donation-cdc/Dependencies.toml` (currently a stray untracked file from an earlier, incomplete attempt — let `bal build` regenerate it against the real project; do not hand-edit it)

**Interfaces:**
- Consumes: declarative syntax from Task 6/7.
- Produces: a standalone example project, matching the target syntax in the task's spec exactly.

- [ ] **Step 1: Create `examples/donation-cdc/Ballerina.toml`**

Mirror `examples/cdc/Ballerina.toml`:
```toml
[package]
org = "example"
name = "salesforce_pubsub_donation_cdc"
version = "0.1.0"
distribution = "2201.12.0"

[[dependency]]
org = "ballerinax"
name = "salesforce.pubsub"
version = "0.1.0"
repository = "local"
```
(Package name matches the `example`/`salesforce_pubsub_donation_cdc` name already recorded in the stray `Dependencies.toml`.)

- [ ] **Step 2: Create `examples/donation-cdc/main.bal`**

```ballerina
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
// its topic chosen at runtime should use programmatic `attach()` instead.
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
```

- [ ] **Step 3: Build it**

Run: `cd examples/donation-cdc && bal build`
Expected: builds cleanly, regenerating `Dependencies.toml`.

- [ ] **Step 4: Commit**

```bash
git add examples/donation-cdc/Ballerina.toml examples/donation-cdc/main.bal examples/donation-cdc/Dependencies.toml
git commit -m "Add declarative Donation CDC example project"
```

---

## Task 9: Full regression pass

**Files:** none (verification only)

- [ ] **Step 1: Run the full package test suite**

Run: `cd ballerina && bal test`
Expected: all tests pass (baseline was 118 passing, 0 failing before this plan; expect that count plus the new tests from Tasks 3 and 5, all passing).

- [ ] **Step 2: Build every example**

Run:
```bash
for d in examples/listen examples/cdc examples/multi-topic examples/publish examples/donation-cdc; do
    (cd "$d" && bal build)
done
```
Expected: all five build cleanly.

- [ ] **Step 3: Report final state**

Summarize: test pass/fail counts, any warnings introduced, and confirm no `Dependencies.toml` was hand-edited (all were regenerated by `bal build`).

---

## Outcome (2026-09-16)

Task 4 ("Convert `Listener` to `public isolated class`") was **dropped** after
implementation and empirical testing showed it is incompatible with the
target declarative syntax:

- Ballerina forbids an `isolated class`/`isolated object` from storing a
  value of a non-isolated type in any field, final or `lock`-protected —
  confirmed directly (not from docs) by attempting it and reading the
  compiler's own diagnostics ("invalid attempt to transfer a value into/out
  of a lock statement").
- `pubsub:Listener` must store caller-supplied `Service` values (and the
  gRPC stream client bundled with each) in its own state. `Service` is a
  plain `service object {}`, not isolated.
- The only way to make this legal is to declare `Service` as
  `isolated service object { remote isolated function onEvent(...); }`.
  This was verified against the *actual* `service on listener { ... }`
  declaration form (not just an anonymous value expression): the compiler
  rejects it with "service declaration does not implement all required
  constructs of type: Service" unless `isolated` is written explicitly on
  `onEvent`/`onError` — Ballerina's automatic isolation inference does not
  retroactively satisfy an explicitly `isolated`-qualified interface.
- The spec's own target example has no `isolated` keyword. Requiring it
  would be a breaking change for every existing and new `Service`
  implementation (declarative and programmatic), not just an internal
  detail.
- The built-in `grpc:Listener` this connector wraps sidesteps the same
  problem by storing attached services through a native (Java) call, never
  in a Ballerina-level field — not an option for this pure-Ballerina
  connector.

Given this, `Listener` stays a plain (non-isolated) `public class`, per an
explicit decision made with the user after walking through the tradeoff.
Everything else in this plan (declarative attach, `ServiceSubscriptionConfig`
+ `topic`, `attach()` topic resolution, `detach()`, examples, tests) proceeds
unchanged.

Along the way, `subscriptionConfigFor`'s first parameter was narrowed from
`ListenerConfig` to `SubscriptionConfig` (the listener-wide default only),
since the full `ListenerConfig` was never needed by the function body — a
simplification, not part of the original isolation work.

The `{*SubscriptionConfig; topic}` annotation record ended up named
`ServiceSubscriptionConfig` (not `ServiceTopicConfig`, the initial pick) —
chosen with the user directly, to avoid colliding with or being confused for
the `ServiceConfig` annotation name.

A `detach(Service s) returns error?` method was added to `Listener` — not in
the original plan, but required by Ballerina's built-in listener contract for
`listener` variable declarations (the declarative syntax) to type-check at
all; it was surfaced only once the actual `listener pubsub:Listener x = ...`
form was compiled for the first time.

## Self-Review Notes

- **Spec coverage:** `Listener` → `isolated class` (Task 4); `SubscriptionConfig.topic` (Task 1); `attach()` resolution rules including exact-match conflict and duplicate-topic retention (Tasks 2-3); `subscriptionConfigFor` split return (Task 2); Donation CDC declarative update — recreated since it didn't exist (Task 8); test plan's six bullets — one-topic no-path e2e (Task 5), two declarative topics independent (Task 5), missing/empty/duplicate/conflicting local rejection (Task 3), existing programmatic/flattened-config regression (Tasks 2-4 rerun full suite), Donation sample + example builds (Tasks 7-9).
- **Placeholder scan:** every step carries literal code; the one open question (exact compiler-supplied path for a bare `service on`) is resolved empirically in Task 6 rather than assumed, per the plan's own stated uncertainty.
- **Type consistency:** `subscriptionConfigFor` returns `[SubscriptionConfig, string?]` consistently across Tasks 2, 3, 5; `resolveAttachTopic`/`normalizedAttachTopic` names and signatures match between their Task 3 definition and all call sites.
