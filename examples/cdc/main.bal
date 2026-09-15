import ballerina/http;
import ballerinax/salesforce.pubsub;

configurable string accessToken = ?;
configurable string instanceUrl = ?;
configurable string tenantId = ?;

public function main() returns error? {
    pubsub:Listener events = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: accessToken},
            instanceUrl,
            tenantId
        }
    });
    pubsub:Service handler = service object {
        remote function onEvent(pubsub:Event event) returns error? {
            // For /data/* topics, payload contains {changedData, metadata}.
            pubsub:Payload changedData = check event.payload["changedData"].ensureType();
            pubsub:Payload metadata = check event.payload["metadata"].ensureType();
            _ = changedData;
            _ = metadata;
        }
    };
    check events.attach(handler, "/data/ChangeEvents");
    check events.'start();
}
