// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/grpc;
import ballerina/protobuf;

public const string PUBSUB_API_DESC = "0A107075627375625F6170692E70726F746F120B6576656E746275732E763122C5010A09546F706963496E666F121D0A0A746F7069635F6E616D651801200128095209746F7069634E616D65121F0A0B74656E616E745F67756964180220012809520A74656E616E7447756964121F0A0B63616E5F7075626C697368180320012808520A63616E5075626C69736812230A0D63616E5F737562736372696265180420012808520C63616E537562736372696265121B0A09736368656D615F69641805200128095208736368656D61496412150A067270635F696418062001280952057270634964222D0A0C546F70696352657175657374121D0A0A746F7069635F6E616D651801200128095209746F7069634E616D6522350A0B4576656E7448656164657212100A036B657918012001280952036B657912140A0576616C756518022001280C520576616C7565228A010A0D50726F64756365724576656E74120E0A02696418012001280952026964121B0A09736368656D615F69641802200128095208736368656D61496412180A077061796C6F616418032001280C52077061796C6F616412320A076865616465727318042003280B32182E6576656E746275732E76312E4576656E74486561646572520768656164657273225E0A0D436F6E73756D65724576656E7412300A056576656E7418012001280B321A2E6576656E746275732E76312E50726F64756365724576656E7452056576656E74121B0A097265706C61795F696418022001280C52087265706C61794964227F0A0D5075626C697368526573756C74121B0A097265706C61795F696418012001280C52087265706C6179496412280A056572726F7218022001280B32122E6576656E746275732E76312E4572726F7252056572726F7212270A0F636F7272656C6174696F6E5F6B6579180320012809520E636F7272656C6174696F6E4B657922450A054572726F72122A0A04636F646518012001280E32162E6576656E746275732E76312E4572726F72436F64655204636F646512100A036D736718022001280952036D736722D2010A0C466574636852657175657374121D0A0A746F7069635F6E616D651801200128095209746F7069634E616D65123E0A0D7265706C61795F70726573657418022001280E32192E6576656E746275732E76312E5265706C6179507265736574520C7265706C6179507265736574121B0A097265706C61795F696418032001280C52087265706C6179496412230A0D6E756D5F726571756573746564180420012805520C6E756D52657175657374656412210A0C617574685F72656672657368180520012809520B617574685265667265736822B8010A0D4665746368526573706F6E736512320A066576656E747318012003280B321A2E6576656E746275732E76312E436F6E73756D65724576656E7452066576656E747312280A106C61746573745F7265706C61795F696418022001280C520E6C61746573745265706C6179496412150A067270635F69641803200128095205727063496412320A1570656E64696E675F6E756D5F726571756573746564180420012805521370656E64696E674E756D526571756573746564222C0A0D536368656D6152657175657374121B0A09736368656D615F69641801200128095208736368656D61496422610A0A536368656D61496E666F121F0A0B736368656D615F6A736F6E180120012809520A736368656D614A736F6E121B0A09736368656D615F69641802200128095208736368656D61496412150A067270635F6964180320012809520572706349642286010A0E5075626C69736852657175657374121D0A0A746F7069635F6E616D651801200128095209746F7069634E616D6512320A066576656E747318022003280B321A2E6576656E746275732E76312E50726F64756365724576656E7452066576656E747312210A0C617574685F72656672657368180320012809520B6175746852656672657368227B0A0F5075626C697368526573706F6E736512340A07726573756C747318012003280B321A2E6576656E746275732E76312E5075626C697368526573756C745207726573756C7473121B0A09736368656D615F69641802200128095208736368656D61496412150A067270635F6964180320012809520572706349642288020A134D616E6167656446657463685265717565737412270A0F737562736372697074696F6E5F6964180120012809520E737562736372697074696F6E496412250A0E646576656C6F7065725F6E616D65180220012809520D646576656C6F7065724E616D6512230A0D6E756D5F726571756573746564180320012805520C6E756D52657175657374656412210A0C617574685F72656672657368180420012809520B617574685265667265736812590A18636F6D6D69745F7265706C61795F69645F7265717565737418052001280B32202E6576656E746275732E76312E436F6D6D69745265706C6179526571756573745215636F6D6D69745265706C6179496452657175657374228B020A144D616E616765644665746368526573706F6E736512320A066576656E747318012003280B321A2E6576656E746275732E76312E436F6E73756D65724576656E7452066576656E747312280A106C61746573745F7265706C61795F696418022001280C520E6C61746573745265706C6179496412150A067270635F69641803200128095205727063496412320A1570656E64696E675F6E756D5F726571756573746564180420012805521370656E64696E674E756D526571756573746564124A0A0F636F6D6D69745F726573706F6E736518052001280B32212E6576656E746275732E76312E436F6D6D69745265706C6179526573706F6E7365520E636F6D6D6974526573706F6E7365225E0A13436F6D6D69745265706C617952657175657374122A0A11636F6D6D69745F726571756573745F6964180120012809520F636F6D6D6974526571756573744964121B0A097265706C61795F696418022001280C52087265706C6179496422AC010A14436F6D6D69745265706C6179526573706F6E7365122A0A11636F6D6D69745F726571756573745F6964180120012809520F636F6D6D6974526571756573744964121B0A097265706C61795F696418022001280C52087265706C6179496412280A056572726F7218032001280B32122E6576656E746275732E76312E4572726F7252056572726F7212210A0C70726F636573735F74696D65180420012803520B70726F6365737354696D652A310A094572726F72436F6465120B0A07554E4B4E4F574E1000120B0A075055424C4953481001120A0A06434F4D4D495410022A340A0C5265706C6179507265736574120A0A064C41544553541000120C0A084541524C494553541001120A0A06435553544F4D100232C4030A0650756253756212460A0953756273637269626512192E6576656E746275732E76312E4665746368526571756573741A1A2E6576656E746275732E76312E4665746368526573706F6E73652801300112400A09476574536368656D61121A2E6576656E746275732E76312E536368656D61526571756573741A172E6576656E746275732E76312E536368656D61496E666F123D0A08476574546F70696312192E6576656E746275732E76312E546F706963526571756573741A162E6576656E746275732E76312E546F706963496E666F12440A075075626C697368121B2E6576656E746275732E76312E5075626C697368526571756573741A1C2E6576656E746275732E76312E5075626C697368526573706F6E7365124E0A0D5075626C69736853747265616D121B2E6576656E746275732E76312E5075626C697368526571756573741A1C2E6576656E746275732E76312E5075626C697368526573706F6E736528013001125B0A104D616E6167656453756273637269626512202E6576656E746275732E76312E4D616E616765644665746368526571756573741A212E6576656E746275732E76312E4D616E616765644665746368526573706F6E73652801300142610A20636F6D2E73616C6573666F7263652E6576656E746275732E70726F746F627566420B50756253756250726F746F50015A2E6769746875622E636F6D2F646576656C6F706572666F7263652F7075622D7375622D6170692F676F2F70726F746F620670726F746F33";

public isolated client class PubSubClient {
    *grpc:AbstractClientEndpoint;

    private final grpc:Client grpcClient;

    public isolated function init(string url, *grpc:ClientConfiguration config) returns grpc:Error? {
        self.grpcClient = check new (url, config);
        check self.grpcClient.initStub(self, PUBSUB_API_DESC);
    }

    isolated remote function GetSchema(SchemaRequest|ContextSchemaRequest req) returns SchemaInfo|grpc:Error {
        map<string|string[]> headers = {};
        SchemaRequest message;
        if req is ContextSchemaRequest {
            message = req.content;
            headers = req.headers;
        } else {
            message = req;
        }
        var payload = check self.grpcClient->executeSimpleRPC("eventbus.v1.PubSub/GetSchema", message, headers);
        [anydata, map<string|string[]>] [result, _] = payload;
        return <SchemaInfo>result;
    }

    isolated remote function GetSchemaContext(SchemaRequest|ContextSchemaRequest req) returns ContextSchemaInfo|grpc:Error {
        map<string|string[]> headers = {};
        SchemaRequest message;
        if req is ContextSchemaRequest {
            message = req.content;
            headers = req.headers;
        } else {
            message = req;
        }
        var payload = check self.grpcClient->executeSimpleRPC("eventbus.v1.PubSub/GetSchema", message, headers);
        [anydata, map<string|string[]>] [result, respHeaders] = payload;
        return {content: <SchemaInfo>result, headers: respHeaders};
    }

    isolated remote function GetTopic(TopicRequest|ContextTopicRequest req) returns TopicInfo|grpc:Error {
        map<string|string[]> headers = {};
        TopicRequest message;
        if req is ContextTopicRequest {
            message = req.content;
            headers = req.headers;
        } else {
            message = req;
        }
        var payload = check self.grpcClient->executeSimpleRPC("eventbus.v1.PubSub/GetTopic", message, headers);
        [anydata, map<string|string[]>] [result, _] = payload;
        return <TopicInfo>result;
    }

    isolated remote function GetTopicContext(TopicRequest|ContextTopicRequest req) returns ContextTopicInfo|grpc:Error {
        map<string|string[]> headers = {};
        TopicRequest message;
        if req is ContextTopicRequest {
            message = req.content;
            headers = req.headers;
        } else {
            message = req;
        }
        var payload = check self.grpcClient->executeSimpleRPC("eventbus.v1.PubSub/GetTopic", message, headers);
        [anydata, map<string|string[]>] [result, respHeaders] = payload;
        return {content: <TopicInfo>result, headers: respHeaders};
    }

    isolated remote function Publish(PublishRequest|ContextPublishRequest req) returns PublishResponse|grpc:Error {
        map<string|string[]> headers = {};
        PublishRequest message;
        if req is ContextPublishRequest {
            message = req.content;
            headers = req.headers;
        } else {
            message = req;
        }
        var payload = check self.grpcClient->executeSimpleRPC("eventbus.v1.PubSub/Publish", message, headers);
        [anydata, map<string|string[]>] [result, _] = payload;
        return <PublishResponse>result;
    }

    isolated remote function PublishContext(PublishRequest|ContextPublishRequest req) returns ContextPublishResponse|grpc:Error {
        map<string|string[]> headers = {};
        PublishRequest message;
        if req is ContextPublishRequest {
            message = req.content;
            headers = req.headers;
        } else {
            message = req;
        }
        var payload = check self.grpcClient->executeSimpleRPC("eventbus.v1.PubSub/Publish", message, headers);
        [anydata, map<string|string[]>] [result, respHeaders] = payload;
        return {content: <PublishResponse>result, headers: respHeaders};
    }

    isolated remote function Subscribe() returns SubscribeStreamingClient|grpc:Error {
        grpc:StreamingClient sClient = check self.grpcClient->executeBidirectionalStreaming("eventbus.v1.PubSub/Subscribe");
        return new SubscribeStreamingClient(sClient);
    }

    // The gRPC runtime accepts initial metadata for bidirectional streams, but
    // the stock Ballerina generator does not emit a context overload for this
    // operation. Keep this internal extension beside the generated client so
    // Salesforce's required authentication metadata reaches every new stream.
    isolated remote function SubscribeContext(map<string|string[]> headers) returns SubscribeStreamingClient|grpc:Error {
        grpc:StreamingClient sClient = check self.grpcClient->executeBidirectionalStreaming(
            "eventbus.v1.PubSub/Subscribe", headers);
        return new SubscribeStreamingClient(sClient);
    }

    isolated remote function PublishStream() returns PublishStreamStreamingClient|grpc:Error {
        grpc:StreamingClient sClient = check self.grpcClient->executeBidirectionalStreaming("eventbus.v1.PubSub/PublishStream");
        return new PublishStreamStreamingClient(sClient);
    }

    isolated remote function ManagedSubscribe() returns ManagedSubscribeStreamingClient|grpc:Error {
        grpc:StreamingClient sClient = check self.grpcClient->executeBidirectionalStreaming("eventbus.v1.PubSub/ManagedSubscribe");
        return new ManagedSubscribeStreamingClient(sClient);
    }
}

public isolated client class SubscribeStreamingClient {
    private final grpc:StreamingClient sClient;

    isolated function init(grpc:StreamingClient sClient) {
        self.sClient = sClient;
    }

    isolated remote function sendFetchRequest(FetchRequest message) returns grpc:Error? {
        return self.sClient->send(message);
    }

    isolated remote function sendContextFetchRequest(ContextFetchRequest message) returns grpc:Error? {
        return self.sClient->send(message);
    }

    isolated remote function receiveFetchResponse() returns FetchResponse|grpc:Error? {
        var response = check self.sClient->receive();
        if response is () {
            return response;
        } else {
            [anydata, map<string|string[]>] [payload, _] = response;
            return <FetchResponse>payload;
        }
    }

    isolated remote function receiveContextFetchResponse() returns ContextFetchResponse|grpc:Error? {
        var response = check self.sClient->receive();
        if response is () {
            return response;
        } else {
            [anydata, map<string|string[]>] [payload, headers] = response;
            return {content: <FetchResponse>payload, headers: headers};
        }
    }

    isolated remote function sendError(grpc:Error response) returns grpc:Error? {
        return self.sClient->sendError(response);
    }

    isolated remote function complete() returns grpc:Error? {
        return self.sClient->complete();
    }
}

public isolated client class PublishStreamStreamingClient {
    private final grpc:StreamingClient sClient;

    isolated function init(grpc:StreamingClient sClient) {
        self.sClient = sClient;
    }

    isolated remote function sendPublishRequest(PublishRequest message) returns grpc:Error? {
        return self.sClient->send(message);
    }

    isolated remote function sendContextPublishRequest(ContextPublishRequest message) returns grpc:Error? {
        return self.sClient->send(message);
    }

    isolated remote function receivePublishResponse() returns PublishResponse|grpc:Error? {
        var response = check self.sClient->receive();
        if response is () {
            return response;
        } else {
            [anydata, map<string|string[]>] [payload, _] = response;
            return <PublishResponse>payload;
        }
    }

    isolated remote function receiveContextPublishResponse() returns ContextPublishResponse|grpc:Error? {
        var response = check self.sClient->receive();
        if response is () {
            return response;
        } else {
            [anydata, map<string|string[]>] [payload, headers] = response;
            return {content: <PublishResponse>payload, headers: headers};
        }
    }

    isolated remote function sendError(grpc:Error response) returns grpc:Error? {
        return self.sClient->sendError(response);
    }

    isolated remote function complete() returns grpc:Error? {
        return self.sClient->complete();
    }
}

public isolated client class ManagedSubscribeStreamingClient {
    private final grpc:StreamingClient sClient;

    isolated function init(grpc:StreamingClient sClient) {
        self.sClient = sClient;
    }

    isolated remote function sendManagedFetchRequest(ManagedFetchRequest message) returns grpc:Error? {
        return self.sClient->send(message);
    }

    isolated remote function sendContextManagedFetchRequest(ContextManagedFetchRequest message) returns grpc:Error? {
        return self.sClient->send(message);
    }

    isolated remote function receiveManagedFetchResponse() returns ManagedFetchResponse|grpc:Error? {
        var response = check self.sClient->receive();
        if response is () {
            return response;
        } else {
            [anydata, map<string|string[]>] [payload, _] = response;
            return <ManagedFetchResponse>payload;
        }
    }

    isolated remote function receiveContextManagedFetchResponse() returns ContextManagedFetchResponse|grpc:Error? {
        var response = check self.sClient->receive();
        if response is () {
            return response;
        } else {
            [anydata, map<string|string[]>] [payload, headers] = response;
            return {content: <ManagedFetchResponse>payload, headers: headers};
        }
    }

    isolated remote function sendError(grpc:Error response) returns grpc:Error? {
        return self.sClient->sendError(response);
    }

    isolated remote function complete() returns grpc:Error? {
        return self.sClient->complete();
    }
}

public isolated client class PubSubTopicInfoCaller {
    private final grpc:Caller caller;

    public isolated function init(grpc:Caller caller) {
        self.caller = caller;
    }

    public isolated function getId() returns int {
        return self.caller.getId();
    }

    isolated remote function sendTopicInfo(TopicInfo response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendContextTopicInfo(ContextTopicInfo response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendError(grpc:Error response) returns grpc:Error? {
        return self.caller->sendError(response);
    }

    isolated remote function complete() returns grpc:Error? {
        return self.caller->complete();
    }

    public isolated function isCancelled() returns boolean {
        return self.caller.isCancelled();
    }
}

public isolated client class PubSubSchemaInfoCaller {
    private final grpc:Caller caller;

    public isolated function init(grpc:Caller caller) {
        self.caller = caller;
    }

    public isolated function getId() returns int {
        return self.caller.getId();
    }

    isolated remote function sendSchemaInfo(SchemaInfo response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendContextSchemaInfo(ContextSchemaInfo response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendError(grpc:Error response) returns grpc:Error? {
        return self.caller->sendError(response);
    }

    isolated remote function complete() returns grpc:Error? {
        return self.caller->complete();
    }

    public isolated function isCancelled() returns boolean {
        return self.caller.isCancelled();
    }
}

public isolated client class PubSubPublishResponseCaller {
    private final grpc:Caller caller;

    public isolated function init(grpc:Caller caller) {
        self.caller = caller;
    }

    public isolated function getId() returns int {
        return self.caller.getId();
    }

    isolated remote function sendPublishResponse(PublishResponse response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendContextPublishResponse(ContextPublishResponse response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendError(grpc:Error response) returns grpc:Error? {
        return self.caller->sendError(response);
    }

    isolated remote function complete() returns grpc:Error? {
        return self.caller->complete();
    }

    public isolated function isCancelled() returns boolean {
        return self.caller.isCancelled();
    }
}

public isolated client class PubSubFetchResponseCaller {
    private final grpc:Caller caller;

    public isolated function init(grpc:Caller caller) {
        self.caller = caller;
    }

    public isolated function getId() returns int {
        return self.caller.getId();
    }

    isolated remote function sendFetchResponse(FetchResponse response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendContextFetchResponse(ContextFetchResponse response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendError(grpc:Error response) returns grpc:Error? {
        return self.caller->sendError(response);
    }

    isolated remote function complete() returns grpc:Error? {
        return self.caller->complete();
    }

    public isolated function isCancelled() returns boolean {
        return self.caller.isCancelled();
    }
}

public isolated client class PubSubManagedFetchResponseCaller {
    private final grpc:Caller caller;

    public isolated function init(grpc:Caller caller) {
        self.caller = caller;
    }

    public isolated function getId() returns int {
        return self.caller.getId();
    }

    isolated remote function sendManagedFetchResponse(ManagedFetchResponse response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendContextManagedFetchResponse(ContextManagedFetchResponse response) returns grpc:Error? {
        return self.caller->send(response);
    }

    isolated remote function sendError(grpc:Error response) returns grpc:Error? {
        return self.caller->sendError(response);
    }

    isolated remote function complete() returns grpc:Error? {
        return self.caller->complete();
    }

    public isolated function isCancelled() returns boolean {
        return self.caller.isCancelled();
    }
}

public type ContextManagedFetchResponseStream record {|
    stream<ManagedFetchResponse, error?> content;
    map<string|string[]> headers;
|};

public type ContextPublishResponseStream record {|
    stream<PublishResponse, error?> content;
    map<string|string[]> headers;
|};

public type ContextFetchRequestStream record {|
    stream<FetchRequest, error?> content;
    map<string|string[]> headers;
|};

public type ContextManagedFetchRequestStream record {|
    stream<ManagedFetchRequest, error?> content;
    map<string|string[]> headers;
|};

public type ContextFetchResponseStream record {|
    stream<FetchResponse, error?> content;
    map<string|string[]> headers;
|};

public type ContextPublishRequestStream record {|
    stream<PublishRequest, error?> content;
    map<string|string[]> headers;
|};

public type ContextManagedFetchResponse record {|
    ManagedFetchResponse content;
    map<string|string[]> headers;
|};

public type ContextTopicRequest record {|
    TopicRequest content;
    map<string|string[]> headers;
|};

public type ContextPublishResponse record {|
    PublishResponse content;
    map<string|string[]> headers;
|};

public type ContextSchemaRequest record {|
    SchemaRequest content;
    map<string|string[]> headers;
|};

public type ContextFetchRequest record {|
    FetchRequest content;
    map<string|string[]> headers;
|};

public type ContextManagedFetchRequest record {|
    ManagedFetchRequest content;
    map<string|string[]> headers;
|};

public type ContextFetchResponse record {|
    FetchResponse content;
    map<string|string[]> headers;
|};

public type ContextPublishRequest record {|
    PublishRequest content;
    map<string|string[]> headers;
|};

public type ContextTopicInfo record {|
    TopicInfo content;
    map<string|string[]> headers;
|};

public type ContextSchemaInfo record {|
    SchemaInfo content;
    map<string|string[]> headers;
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type ManagedFetchResponse record {|
    ConsumerEvent[] events = [];
    byte[] latest_replay_id = [];
    string rpc_id = "";
    int pending_num_requested = 0;
    CommitReplayResponse commit_response = {};
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type TopicRequest record {|
    string topic_name = "";
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type PublishResponse record {|
    PublishResult[] results = [];
    string schema_id = "";
    string rpc_id = "";
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type SchemaRequest record {|
    string schema_id = "";
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type CommitReplayResponse record {|
    string commit_request_id = "";
    byte[] replay_id = [];
    Error 'error = {};
    int process_time = 0;
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type Error record {|
    ErrorCode code = UNKNOWN;
    string msg = "";
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type EventHeader record {|
    string key = "";
    byte[] value = [];
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type ProducerEvent record {|
    string id = "";
    string schema_id = "";
    byte[] payload = [];
    EventHeader[] headers = [];
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type FetchResponse record {|
    ConsumerEvent[] events = [];
    byte[] latest_replay_id = [];
    string rpc_id = "";
    int pending_num_requested = 0;
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type PublishRequest record {|
    string topic_name = "";
    ProducerEvent[] events = [];
    string auth_refresh = "";
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type TopicInfo record {|
    string topic_name = "";
    string tenant_guid = "";
    boolean can_publish = false;
    boolean can_subscribe = false;
    string schema_id = "";
    string rpc_id = "";
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type CommitReplayRequest record {|
    string commit_request_id = "";
    byte[] replay_id = [];
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type PublishResult record {|
    byte[] replay_id = [];
    Error 'error = {};
    string correlation_key = "";
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type ConsumerEvent record {|
    ProducerEvent event = {};
    byte[] replay_id = [];
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type FetchRequest record {|
    string topic_name = "";
    ReplayPreset replay_preset = LATEST;
    byte[] replay_id = [];
    int num_requested = 0;
    string auth_refresh = "";
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type ManagedFetchRequest record {|
    string subscription_id = "";
    string developer_name = "";
    int num_requested = 0;
    string auth_refresh = "";
    CommitReplayRequest commit_replay_id_request = {};
|};

@protobuf:Descriptor {value: PUBSUB_API_DESC}
public type SchemaInfo record {|
    string schema_json = "";
    string schema_id = "";
    string rpc_id = "";
|};

public enum ErrorCode {
    UNKNOWN, PUBLISH, COMMIT
}

public enum ReplayPreset {
    LATEST, EARLIEST, CUSTOM
}
