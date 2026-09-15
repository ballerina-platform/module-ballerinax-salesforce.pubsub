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
            // Convert event.payload to the application record type when needed.
        }
    };
    check events.attach(handler, "/event/Order_Notification__e");
    check events.'start();
}
