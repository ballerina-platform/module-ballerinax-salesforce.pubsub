# Salesforce Pub/Sub examples

- [`publish`](publish): publishes a platform event through a topic-bound Publisher.
- [`listen`](listen): attaches a sequential listener service for one platform event topic.
- [`cdc`](cdc): consumes Change Data Capture events with normalized changed data and metadata.

Provide `accessToken`, `instanceUrl`, and `tenantId` through Ballerina configuration before running either example. `tenantId` is the Salesforce org ID, not the user ID returned by the OAuth identity endpoint. For example, create an uncommitted `Config.toml` in an example directory:

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

The listener is long-running. Stop it with the normal Ballerina lifecycle when you are done. Both examples use a bearer token only; renewable OAuth grant support follows the shared Salesforce token-provider release.
