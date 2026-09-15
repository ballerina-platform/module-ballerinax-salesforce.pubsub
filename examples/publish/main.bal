import ballerina/http;
import ballerinax/salesforce.pubsub;

configurable string accessToken = ?;
configurable string instanceUrl = ?;
configurable string tenantId = ?;

public function main() returns error? {
    pubsub:Publisher orders = check new ({
        connection: {
            auth: <http:BearerTokenConfig>{token: accessToken},
            instanceUrl,
            tenantId
        },
        topic: "/event/Order_Notification__e"
    });

    pubsub:PublishResult[] results = check orders->publish([
        {payload: {
            "CreatedDate": 0,
            "CreatedById": "",
            "Order_Id__c": "ORDER-1042",
            "Status__c": "SHIPPED"
        }}
    ]);
    foreach pubsub:PublishResult result in results {
        if result.itemError is error {
            return result.itemError;
        }
    }
}
