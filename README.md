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

The Publisher looks up current topic metadata before each batch, retrieves its writer schema through the process-wide tenant/schema cache, generates missing correlation IDs, and checks encoded protobuf event/request sizes against the 1 MB and 4 MB hard limits.
Payload fields must satisfy the topic's current Avro writer schema. In this
example, `CreatedDate` and `CreatedById` are non-nullable fields in the sandbox
event schema.

## Consume events

Attach one service per canonical topic path to a Listener. Topic delivery is sequential: handler success → exact replay checkpoint → replacement credit. The default in-memory checkpoint store lasts only for the process lifetime.

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

Without a checkpoint, a topic starts at `LATEST`. An expired checkpoint recovers from `EARLIEST` by default. Because an application side effect and checkpoint save are not atomic, an event can be redelivered after a crash; consumers must make side effects idempotent. The default buffer size is 10 and the connector uses at most one internal liveness reserve slot.

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

Sandbox tests are disabled by default. Copy
`ballerina/tests/Config.toml.example` to `ballerina/tests/Config.toml` and fill
in `EP_URL`, `ACCESS_TOKEN`, `CLIENT_ID`, `CLIENT_SECRET`, `REFRESH_URL`,
`SF_USERNAME`, `SF_PASSWORD`, and `PUBSUB_TENANT_ID` through the corresponding
`sandbox*` values. `Config.toml` is gitignored and must never be committed.
Run the non-destructive authentication and platform-event checks with
`bal test --groups sandbox` from `ballerina`. The suite uses unique correlation
IDs; platform events are intentionally not deleted. Account CDC lifecycle
coverage remains in the separate `sandbox-cdc` group because it creates and
deletes one Account.

See [examples](examples/README.md) for standalone publish and listener source.
