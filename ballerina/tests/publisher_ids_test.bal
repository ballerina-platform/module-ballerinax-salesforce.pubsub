import ballerina/test;

// This fails if omitted correlation IDs reach Salesforce empty, preventing the
// connector from identifying inputs after an ambiguous Publish response.
@test:Config {}
function testPublisherAssignsMissingCorrelationIds() {
    PublishEvent[] events = [{payload: {"Name": "first"}}, {id: "caller-id", payload: {"Name": "second"}}];

    PublishEvent[] identified = ensurePublishEventIds(events);

    string? generated = identified[0].id;
    test:assertTrue(generated is string);
    if generated is string {
        test:assertTrue(generated.length() > 0);
    }
    test:assertEquals(identified[1].id, "caller-id");
}
