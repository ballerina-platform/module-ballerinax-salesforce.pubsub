import ballerina/http;
import ballerina/io;
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

    // Each attached topic gets its own independent stream, cursor, and
    // sequential delivery; a slow or stuck handler on one topic never blocks
    // the other's progress.
    pubsub:Service orders = service object {
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
    };

    // A service is not required to define onError; onEvent alone is valid.
    pubsub:Service shipments = service object {
        remote function onEvent(pubsub:Event event) returns error? {
            // Convert event.payload to the application record type when needed.
        }
    };

    check events.attach(orders, "/event/Order_Notification__e");
    check events.attach(shipments, "/event/Shipment_Notification__e");
    check events.'start();
}
