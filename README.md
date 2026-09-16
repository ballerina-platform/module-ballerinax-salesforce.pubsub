# Ballerina Salesforce Pub/Sub connector

`ballerinax/salesforce.pubsub` publishes Salesforce platform events and consumes Pub/Sub API event streams through separate `Publisher` and `Listener` APIs. Generated gRPC records, authentication metadata, and Avro bytes remain inside the connector.

## Status

This is an in-progress first-cut implementation. Bearer-token, refresh-token,
client-credentials, and password-grant authentication are implemented inside
this package, alongside unary publishing, dynamic Avro envelopes, in-memory
replay checkpoints, and topic-scoped sequential consumption. Pub/Sub reuses
the public Salesforce OAuth configuration and TokenStore contracts, but does
not depend on the Salesforce connector's private CometD token manager.

## Configure a connection

Supply the Salesforce instance URL, the Salesforce **org** ID (normally starts with `00D`), and the Pub/Sub endpoint. The connector sends `accesstoken`, `instanceurl`, and `tenantid` internally for every unary call and new stream.

```ballerina
import ballerina/http;
import ballerinax/salesforce.pubsub;

pubsub:ConnectionConfig connection = {
    auth: <http:BearerTokenConfig>{token: "${ACCESS_TOKEN}"},
    instanceUrl: "https://your-org.my.salesforce.com",
    tenantId: "00D..."
};
```

Do not commit access tokens, refresh tokens, or event payloads to source control. Use configurable values or deployment secret management instead.

For a refresh-token grant, the connector retains the current access token and
any Salesforce-rotated refresh token in `tokenStore`. The default is
`salesforce:InMemoryTokenStore`, suitable for a single process. Supply a
shared `salesforce:TokenStore` when a rotated token must survive a restart or
be coordinated between processes. When Salesforce omits `expires_in`, set
`sessionTimeout` to your org's session lifetime; it defaults to 900 seconds
and the connector renews 60 seconds early.

`ConnectionConfig.grpcConfig` accepts Ballerina `grpc:ClientConfiguration` for
TLS trust material, client certificates, proxy/pool settings, compression, and
inbound message limits. The connector applies `connectionTimeout` to every
owned channel, including channels created for a Listener restart.

```ballerina
import ballerina/grpc;

pubsub:ConnectionConfig connection = {
    auth: <http:BearerTokenConfig>{token: accessToken},
    instanceUrl: instanceUrl,
    tenantId: tenantId,
    connectionTimeout: 30,
    grpcConfig: <grpc:ClientConfiguration>{
        secureSocket: {cert: "./salesforce-ca.pem"},
        maxInboundMessageSize: 8 * 1024 * 1024
    }
};
```

## Publish platform events

Each Publisher is bound to one topic. `publish` returns a result for each input when Salesforce returns a definitive result set. Local Avro failures and Salesforce item failures remain item-level errors. If a Publish response is lost or otherwise uncertain, the connector returns a request-level `ambiguous Publish outcome` error with `topic` and `eventIds` detail for the events included in that one RPC; it never republishes automatically.

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

The Publisher looks up current topic metadata before each batch, retrieves its writer schema through the process-wide tenant/schema cache, and generates missing correlation IDs.
Payload fields must satisfy the topic's current Avro writer schema. In this
example, `CreatedDate` and `CreatedById` are non-nullable fields in the sandbox
event schema.

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

Declare one service per canonical topic path on a Listener, with the topic supplied by `@pubsub:ServiceConfig`. Topic delivery is sequential: handler success → exact replay checkpoint → replacement credit. The default in-memory checkpoint store lasts only for the process lifetime.

```ballerina
listener pubsub:Listener events = check new ({connection});

@pubsub:ServiceConfig {
    topic: "/event/Order_Notification__e"
}
service on events {
    remote function onEvent(pubsub:Event event) returns error? {
        // Process event.payload. Return an error to retry this event in place.
    }
}
```

A topic known only at runtime is attached programmatically instead, without a `@ServiceConfig` topic:

```ballerina
pubsub:Listener events = check new ({connection});
pubsub:Service handler = service object {
    remote function onEvent(pubsub:Event event) returns error? {
        // Process event.payload. Return an error to retry this event in place.
    }
};
check events.attach(handler, "/event/Order_Notification__e");
check events.'start();
```

Without a checkpoint, a topic starts at `LATEST`. An expired checkpoint recovers from `EARLIEST` by default. Because an application side effect and checkpoint save are not atomic, an event can be redelivered after a crash; consumers must make side effects idempotent. The default buffer size is 10, and outstanding protocol request credit never exceeds Salesforce's own maximum of 100.

A Listener may have several topics attached -- declaratively, programmatically, or a mix of both -- each with its own independent stream, cursor, and delivery progress -- a slow handler on one topic never blocks another's progress. However, failure handling is listener-wide, not per-topic: once one topic exhausts its retry budget or hits a terminal (non-retryable) error, the whole Listener stops every attached topic's stream, not just the failing one. A service may optionally define `remote function onError(pubsub:ListenerError err) returns error?`; the Listener invokes it on every attached service that defines it (a service with only `onEvent` remains valid) before it finishes stopping. `getLastError()` returns the terminal diagnostic afterward, including which topic and operation failed. See the [multi-topic example](examples/multi-topic) for both an `onError`-aware and a plain `onEvent`-only service attached side by side.

A declarative service's topic (from `@pubsub:ServiceConfig`) and a programmatic `attach()` topic can both be given for the same service only if they match exactly; a mismatch is rejected before any network activity, as is a missing topic (neither source given) or an empty one.

The connector never exits the application process on any failure, including retry exhaustion or a terminal Listener error; it only stops its own streams. Restarting the process, supervising the Listener, and deciding whether/when to restart it (for example under Kubernetes pod replacement) are all the application's responsibility, not the connector's. Restarting with the default `InMemoryReplayStore` starts every topic without its prior checkpoint (`LATEST`, or `CUSTOM` only if you seed one), since that store's state does not survive the process; supply a durable `ReplayStore` implementation if a restart must resume from the last saved position.

## Consume Change Data Capture events

For `/data/*` topics, the Listener normalizes Salesforce `ChangeEventHeader`
bitmaps before it invokes `onEvent`. The body has `changedData` and `metadata`
fields: `changedData` contains changed values, including explicit nulls, while
metadata retains the header and exposes `changedFields`, `nulledFields`, and
`diffFields` as field names. Standard platform and real-time event payloads
remain dynamic Avro-decoded `Payload` values. See the [CDC listener
example](examples/cdc) for a starting point.

## Build and test

Run deterministic package tests without Salesforce credentials:

```bash
cd ballerina
bal test
```

The suite starts a self-signed TLS Pub/Sub fixture on `localhost:19091` to
exercise generated unary and streaming gRPC transport. Ensure that port is
available when running the suite.

The repository Gradle build uses Docker for Ballerina packaging:

```bash
./gradlew build
```

### Shared-org sandbox tests

Sandbox tests follow the existing Salesforce connector convention and read
`EP_URL`, `ACCESS_TOKEN`, `CLIENT_ID`, `CLIENT_SECRET`, `REFRESH_TOKEN`,
`REFRESH_URL`, `SF_USERNAME`, and `SF_PASSWORD` directly from the environment.
They are enabled only when the required Salesforce credentials are present.
The reusable CI workflows inherit the existing protected org secrets; no
additional workflow or credential file is required. The suite uses unique
correlation IDs; platform events are intentionally not deleted. Account CDC
coverage creates and deletes only its own uniquely named Account.

See [examples](examples/README.md) for standalone publish and listener source.
