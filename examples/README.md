# Salesforce Pub/Sub examples

- [`publish`](publish): publishes a platform event through a topic-bound Publisher.
- [`listen`](listen): declares a listener service for one platform event topic.
- [`cdc`](cdc): consumes Change Data Capture events with normalized changed data and metadata.
- [`multi-topic`](multi-topic): declares two independent topics on one Listener, one with an optional `onError` callback and one without.
- [`donation-cdc`](donation-cdc): a standalone Donation CDC listener with its topic as a compile-time literal; change it directly in source for a differently named object/channel.

`listen`, `cdc`, `multi-topic`, and `donation-cdc` use the declarative form: a `service /event/Topic__e on listener` declaration, with the canonical topic as its literal path. The programmatic form (`check listener.attach(service, topic)`) remains available and is the right choice when the topic is only known at runtime -- a declarative service path, like an annotation value, must be a compile-time constant.

Provide `accessToken`, `instanceUrl`, and `tenantId` through Ballerina configuration before running any example. `tenantId` is the Salesforce org ID, not the user ID returned by the OAuth identity endpoint. For example, create an uncommitted `Config.toml` in an example directory:

```toml
accessToken = "..."
instanceUrl = "https://your-org.my.salesforce.com"
tenantId = "00D..."
```

Build the connector package and push it to the local Ballerina repository before building examples against a working tree:

```bash
cd ../ballerina
bal pack
bal push --repository=local
cd ../examples/publish
bal run
```

A listener example is long-running; stop it with the normal Ballerina lifecycle when you are done. Every example shown here uses a bearer token for simplicity, but the connector also supports refresh-token, client-credentials, and password grants through the same `connection.auth` field -- see the root [README](../README.md#configure-a-connection) for the renewable-grant configuration shape.
