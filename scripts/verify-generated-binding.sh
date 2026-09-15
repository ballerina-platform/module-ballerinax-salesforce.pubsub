#!/usr/bin/env bash
# Verifies the internal Ballerina gRPC binding against the vendored Salesforce
# Pub/Sub API proto. This intentionally does not modify checked-in sources.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "${temporary_dir}"' EXIT

bal grpc --input "${repo_root}/proto/pubsub_api.proto" --output "${temporary_dir}"

python3 - "${temporary_dir}/pubsub_api_pb.bal" <<'PY'
from pathlib import Path
import sys

binding = Path(sys.argv[1])
source = binding.read_text()
needle = '''    isolated remote function Subscribe() returns SubscribeStreamingClient|grpc:Error {
        grpc:StreamingClient sClient = check self.grpcClient->executeBidirectionalStreaming("eventbus.v1.PubSub/Subscribe");
        return new SubscribeStreamingClient(sClient);
    }
'''
overlay = '''
    // The gRPC runtime accepts initial metadata for bidirectional streams, but
    // the stock Ballerina generator does not emit a context overload for this
    // operation. Keep this internal extension beside the generated client so
    // Salesforce's required authentication metadata reaches every new stream.
    isolated remote function SubscribeContext(map<string|string[]> headers) returns SubscribeStreamingClient|grpc:Error {
        grpc:StreamingClient sClient = check self.grpcClient->executeBidirectionalStreaming(
            "eventbus.v1.PubSub/Subscribe", headers);
        return new SubscribeStreamingClient(sClient);
    }
'''
if needle not in source:
    raise SystemExit("Ballerina generator output changed around PubSubClient.Subscribe")
binding.write_text(source.replace(needle, needle + overlay, 1))
PY

cmp "${temporary_dir}/pubsub_api_pb.bal" "${repo_root}/ballerina/modules/internal/pubsub_api_pb.bal"
echo "Generated Pub/Sub binding matches the vendored proto and required metadata overlay."
