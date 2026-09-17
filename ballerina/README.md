## Overview

[Salesforce Pub/Sub API](https://developer.salesforce.com/docs/platform/pub-sub-api/overview) is a gRPC-based, protocol-buffer API that publishes and consumes platform events, custom notifications, and Change Data Capture (CDC) events using Apache Avro binary encoding. It replaces the older CometD-based streaming API with a single high-throughput streaming interface for real-time event-driven integration.

The `ballerinax/salesforce.pubsub` package provides a `Publisher` client and a `Listener` service for the Pub/Sub API. A `Publisher` publishes events to one topic with per-event results and batch chunking. A `Listener` subscribes to one or more topics with exactly-once-in-order delivery per topic, automatic replay checkpointing, and normalized Change Data Capture bodies. Generated gRPC records, OAuth token lifecycle, and Avro encoding/decoding stay internal to the package.

> **Status**: This is an in-progress first-cut implementation. Bearer-token, refresh-token, client-credentials, and password-grant authentication, unary publishing, dynamic Avro envelopes, in-memory replay checkpoints, and topic-scoped sequential consumption are implemented. The package reuses the public `ballerinax/salesforce` OAuth configuration and `TokenStore` contracts but does not depend on that connector's CometD token manager.

### Key features

- `Publisher` and `Listener` clients backed directly by the Pub/Sub gRPC/Avro wire protocol
- Bearer-token, refresh-token, client-credentials, and password-grant OAuth2 authentication, with a pluggable `TokenStore` for rotated refresh tokens
- Batch publishing with automatic request-size chunking and per-event results, so one ambiguous chunk never blocks the rest of a batch
- Topic-scoped sequential delivery: handler success, exact replay checkpoint, and credit replacement, with an independent stream per attached topic
- A pluggable `ReplayStore` so a durable checkpoint can survive a process restart
- Automatic normalization of Change Data Capture `ChangeEventHeader` bitmaps into `changedData` and `metadata` (`changedFields`, `nulledFields`, `diffFields`)
- Listener-wide `onError` diagnostics that report which topic and operation failed, without leaking credentials or payloads

## Setup guide

### Step 1: Create a Salesforce Connected App

1. In Salesforce Setup, go to **App Manager** and click **New Connected App**.
2. Enable **OAuth Settings** and set a callback URL (any HTTPS URL works for OAuth flows that require one, such as the authorization-code grant used below).
3. Under **Selected OAuth Scopes**, add at least:
   - `Manage user data via APIs (api)`
   - `Perform requests at any time (refresh_token, offline_access)`
4. Save, then wait a few minutes for the app to become active. Open **Manage Consumer Details** to get the **Consumer Key** (client ID) and **Consumer Secret** (client secret).

### Step 2: Obtain an access and refresh token

Any OAuth2 grant supported by `salesforce:OAuth2Config` works (bearer token, refresh token, client credentials, or password grant). For a refresh-token grant:

1. In a browser, log in to Salesforce and open:

   ```
   https://<your-instance>.salesforce.com/services/oauth2/authorize?response_type=code&client_id=<CONSUMER_KEY>&redirect_uri=<CALLBACK_URL>
   ```

2. Approve access. Salesforce redirects to `<CALLBACK_URL>?code=<CODE>`; copy `<CODE>`.
3. Exchange the code for tokens:

   ```bash
   curl -X POST https://<your-instance>.salesforce.com/services/oauth2/token \
       -d grant_type=authorization_code \
       -d code=<CODE> \
       -d client_id=<CONSUMER_KEY> \
       -d client_secret=<CONSUMER_SECRET> \
       -d redirect_uri=<CALLBACK_URL>
   ```

   The response carries `access_token` and `refresh_token`. The Pub/Sub endpoint itself (`api.pubsub.salesforce.com:7443`) is the same for production and sandbox orgs; only the login host (`login.salesforce.com` vs. `test.salesforce.com`) differs.

You also need your Salesforce **org ID** (`tenantId`, starting with `00D`), found under Setup > Company Information.

### Step 3: Enable the events you need

- To consume standard or custom object changes, enable those objects under Setup > **Change Data Capture**. Their canonical topics are `/data/<ObjectName>ChangeEvent`.
- To publish or consume a custom Platform Event, create one under Setup > **Platform Events** > **New Platform Event**, and define its fields. Its canonical topic is `/event/<EventName>__e`.

## Quickstart

### Step 1: Import the module

```ballerina
import ballerinax/salesforce.pubsub;
```

### Step 2: Configure the connection

Every `Publisher` and `Listener` shares a `pubsub:ConnectionConfig`. The connector sends `accesstoken`, `instanceurl`, and `tenantid` internally as Pub/Sub RPC metadata for every call.

```ballerina
import ballerina/http;

configurable string accessToken = ?;
configurable string instanceUrl = ?;
configurable string tenantId = ?;

pubsub:ConnectionConfig connection = {
    auth: <http:BearerTokenConfig>{token: accessToken},
    instanceUrl,
    tenantId
};
```

For a refresh-token grant, pass an `oauth2:RefreshTokenGrantConfig` as `auth` instead. The connector keeps the current access token, and any Salesforce-rotated refresh token, in `connection.tokenStore` (an in-memory store by default); supply a shared `salesforce:TokenStore` when a rotated token must survive a restart. Do not commit access tokens, refresh tokens, or event payloads to source control -- use configurable values, as above, or deployment secret management instead.

### Step 3: Publish events

A `Publisher` is bound to one topic. It looks up current topic metadata, retrieves the topic's Avro writer schema, and generates a correlation ID for any event that omits one.

```ballerina
pubsub:Publisher orders = check new ({connection, topic: "/event/Order_Notification__e"});

pubsub:PublishResult[] results = check orders->publish([
    {payload: {
        CreatedDate: 0,
        CreatedById: "",
        Order_Id__c: "ORDER-1042",
        Status__c: "SHIPPED"
    }}
]);
```

Payload fields must satisfy the topic's current Avro writer schema. `publish` returns a per-event result whenever Salesforce returns a definitive result set; a lost or otherwise uncertain Publish response instead surfaces as a request-level `ambiguous Publish outcome` error carrying the affected `topic` and `eventIds` -- the connector never republishes automatically, so the caller decides whether to retry those IDs.

### Step 4: Consume events

Declare one service per canonical topic path on a `Listener`. Delivery within a topic is sequential: handler success, then an exact replay checkpoint, then credit for the next event.

```ballerina
listener pubsub:Listener events = check new ({connection});

service /event/Order_Notification__e on events {
    remote function onEvent(pubsub:Event event) returns error? {
        // Process event.payload. Returning an error retries this event in place.
    }
}
```

A topic known only at runtime is attached programmatically instead, since a declarative service path must be a compile-time literal:

```ballerina
pubsub:Listener events = check new ({connection});
pubsub:Service handler = service object {
    remote function onEvent(pubsub:Event event) returns error? {
        // Process event.payload.
    }
};
check events.attach(handler, topicChosenAtRuntime);
check events.'start();
```

Without a checkpoint, a topic starts at `LATEST`; an expired checkpoint recovers from `EARLIEST` by default. Because an application side effect and its checkpoint save are not atomic, an event can be redelivered after a crash -- make handler side effects idempotent. A service can optionally define `remote function onError(pubsub:ListenerError err) returns error?` to observe a terminal, listener-wide failure (one topic exhausting its retry budget or hitting a non-retryable error stops every attached topic's stream, not just the failing one).

### Step 5: Consume Change Data Capture events

For `/data/*` topics, the Listener normalizes the Salesforce `ChangeEventHeader` bitmaps before invoking `onEvent`. The event body carries `changedData` (changed values, including explicit nulls) and `metadata` (the original header, plus `changedFields`, `nulledFields`, and `diffFields` as field names):

```ballerina
service /data/AccountChangeEvent on events {
    remote function onEvent(pubsub:Event event) returns error? {
        // event.payload.changedData / event.payload.metadata
    }
}
```

### Step 6: Run the Ballerina application

```bash
bal run
```

## Report issues

To report bugs, request new features, start new discussions, view the release notes, and review the module's development lifecycle, visit the [Ballerina library repository](https://github.com/ballerina-platform/ballerina-library).
