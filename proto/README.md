# Salesforce Pub/Sub API protocol source

`pubsub_api.proto` is vendored from Salesforce's public
[`forcedotcom/pub-sub-api`](https://github.com/forcedotcom/pub-sub-api) repository.
It was retrieved from the `main` branch on 2026-09-15 and has SHA-256:

```text
0a651295ff3ff13551b3ae7cb6fe87cf91b749a8ae33b6079617dbc62f304eb5
```

Regenerate and verify the internal Ballerina binding with:

```bash
./scripts/verify-generated-binding.sh
```

The Ballerina gRPC generator creates the base
`ballerina/modules/internal/pubsub_api_pb.bal` file. The checked-in version
also contains `SubscribeContext`, a small internal overlay because the current
generator does not provide metadata headers for bidirectional streaming calls.
The verifier applies that deterministic overlay to a temporary generated file
and compares it byte-for-byte with the checked-in binding.
