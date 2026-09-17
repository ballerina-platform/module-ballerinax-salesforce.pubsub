# Ballerina Salesforce Pub/Sub connector

[![Build](https://github.com/ballerina-platform/module-ballerinax-salesforce.pubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-salesforce.pubsub/actions/workflows/ci.yml)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-salesforce.pubsub.svg)](https://github.com/ballerina-platform/module-ballerinax-salesforce.pubsub/commits/main)
[![GitHub Issues](https://img.shields.io/github/issues/ballerina-platform/ballerina-library/module/salesforce.pubsub.svg?label=Open%20Issues)](https://github.com/ballerina-platform/ballerina-library/labels/module%2Fsalesforce.pubsub)

## Overview

[Salesforce Pub/Sub API](https://developer.salesforce.com/docs/platform/pub-sub-api/overview) is a gRPC-based, Avro-encoded streaming API for publishing and consuming platform events, custom notifications, and Change Data Capture events, replacing the older CometD-based streaming API.

`ballerinax/salesforce.pubsub` publishes Salesforce platform events and consumes Pub/Sub API event streams through separate `Publisher` and `Listener` APIs. Generated gRPC records, authentication metadata, and Avro bytes remain inside the connector.

This is an in-progress first-cut implementation. Bearer-token, refresh-token,
client-credentials, and password-grant authentication are implemented inside
this package, alongside unary publishing, dynamic Avro envelopes, in-memory
replay checkpoints, and topic-scoped sequential consumption. Pub/Sub reuses
the public Salesforce OAuth configuration and TokenStore contracts, but does
not depend on the Salesforce connector's private CometD token manager.

For setup instructions and a quickstart, go to the [`salesforce.pubsub` package](ballerina/README.md).

## Publish platform events

`publish` returns a result for each input when Salesforce returns a definitive result set. Local Avro failures and Salesforce item failures remain item-level errors. If a Publish response is lost or otherwise uncertain, the connector returns a request-level `ambiguous Publish outcome` error with `topic` and `eventIds` detail for the events included in that one RPC; it never republishes automatically.

A batch is split across more than one `Publish` RPC once it exceeds
`targetRequestSizeBytes` (a soft 3 MB target by default); each chunk is
attempted independently, so one ambiguous chunk never stops the others from
being tried. If a chunk's RPC ends without a definitive response, that
chunk's event IDs come back on the request-level `ambiguous Publish outcome`
error, but `definitiveResults` on that same error still carries every result
already obtained from other, unaffected chunks -- so a large multi-chunk
batch never silently loses successful siblings because one chunk was
ambiguous. A batch is never automatically resubmitted; the caller decides
whether to retry the reported IDs. Salesforce's own 1 MB/event and 4 MB/request
limits are not independently re-checked locally -- an oversized event or
request surfaces as Salesforce's own item-level or request-level failure.

## Consume events

A Listener may have several topics attached -- declaratively, programmatically, or a mix of both -- each with its own independent stream, cursor, and delivery progress -- a slow handler on one topic never blocks another's progress. However, failure handling is listener-wide, not per-topic: once one topic exhausts its retry budget or hits a terminal (non-retryable) error, the whole Listener stops every attached topic's stream, not just the failing one. A service may optionally define `remote function onError(pubsub:ListenerError err) returns error?`; the Listener invokes it on every attached service that defines it (a service with only `onEvent` remains valid) before it finishes stopping. `getLastError()` returns the terminal diagnostic afterward, including which topic and operation failed.

The connector never exits the application process on any failure, including retry exhaustion or a terminal Listener error; it only stops its own streams. Restarting the process, supervising the Listener, and deciding whether/when to restart it (for example under Kubernetes pod replacement) are all the application's responsibility, not the connector's. Restarting with the default `InMemoryReplayStore` starts every topic without its prior checkpoint (`LATEST`, or `CUSTOM` only if you seed one), since that store's state does not survive the process; supply a durable `ReplayStore` implementation if a restart must resume from the last saved position.

## Build from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 21. You can download it from either of the following sources:

    * [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
    * [OpenJDK](https://adoptium.net/)

   > **Note:** After installation, remember to set the `JAVA_HOME` environment variable to the directory where JDK was installed.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/).

3. Export a GitHub personal access token with read package permissions, used to resolve `ballerina-platform` packages from GitHub Packages:

    ```bash
    export packageUser=<Username>
    export packagePAT=<Personal access token>
    ```

### Build options

Execute the commands below to build from the source.

1. To build the package:

   ```bash
   ./gradlew clean build
   ```

2. To run the tests:

   ```bash
   ./gradlew clean test
   ```

3. To build without the tests:

   ```bash
   ./gradlew clean build -x test
   ```

4. To debug the package with a remote debugger:

   ```bash
   ./gradlew clean build -Pdebug=<port>
   ```

5. To debug with the Ballerina language:

   ```bash
   ./gradlew clean build -PbalJavaDebug=<port>
   ```

6. Publish the generated artifacts to the local Ballerina Central repository:

    ```bash
    ./gradlew clean build -PpublishToLocalCentral=true
    ```

7. Publish the generated artifacts to the Ballerina Central repository:

   ```bash
   ./gradlew clean build -PpublishToCentral=true
   ```

The package test suite starts a self-signed TLS Pub/Sub fixture on
`localhost:19091` to exercise generated unary and streaming gRPC transport, so
ensure that port is available when running the tests. Building and testing
this package does not require Docker.

### Shared-org sandbox tests

The live shared-org suite is disabled by default. To run it locally, set
`PUBSUB_SANDBOX_TESTS=true` together with `EP_URL`, `CLIENT_ID`,
`CLIENT_SECRET`, `REFRESH_TOKEN`, `REFRESH_URL`, and `SF_TENANT_ID`.
It uses refresh-token OAuth for both REST Account mutations and Pub/Sub API
subscriptions, so it does not depend on an expiring `ACCESS_TOKEN` secret.

Run only the live groups with:

```bash
./gradlew :salesforce.pubsub-ballerina:sandboxTest
```

Trusted upstream pull requests, branch builds, and daily builds set the flag
and credentials automatically. Fork pull requests do not run the live job.
The suite uses `GITHUB_RUN_ID` as its cleanup scope in CI; set
`PUBSUB_SANDBOX_RUN_ID` when rerunning a local interrupted suite to sweep that
run's stale Accounts before tests begin. The CDC scenarios use unique
correlation IDs and delete their own Accounts; platform events are retained
only for Salesforce's normal event-retention period because they cannot be
explicitly deleted.

## Contribute to Ballerina

As an open-source project, Ballerina welcomes contributions from the community.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful links

* For more information, go to the [`salesforce.pubsub` package](https://central.ballerina.io/ballerinax/salesforce.pubsub/latest).
* For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
