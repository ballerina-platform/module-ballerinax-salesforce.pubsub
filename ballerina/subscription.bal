// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License, Version 2.0.

import ballerinax/salesforce.pubsub.internal as wire;

# Builds the first positive FetchRequest for a new Subscribe stream.
#
# + topic - canonical Salesforce topic
# + replayId - last successfully checkpointed opaque cursor, if present
# + position - configured start position when no cursor exists
# + requested - positive initial event credit
# + return - protocol request with CUSTOM only for a stored cursor
public isolated function initialFetchRequest(string topic, byte[]? replayId, ReplayPosition position, int requested)
        returns wire:FetchRequest|error {
    if requested <= 0 {
        return error("initial FetchRequest credit must be greater than zero");
    }
    wire:ReplayPreset preset = position == EARLIEST ? wire:EARLIEST : wire:LATEST;
    if replayId is byte[] {
        preset = wire:CUSTOM;
    }
    return {topic_name: topic, replay_preset: preset, replay_id: replayId ?: [], num_requested: requested};
}

# Builds a fresh stream request after Salesforce rejects an expired checkpoint.
#
# + topic - canonical Salesforce topic
# + position - configured invalid-cursor recovery position
# + requested - positive initial event credit
# + return - request without a stale replay cursor
public isolated function recoveryFetchRequest(string topic, ReplayPosition position, int requested) returns wire:FetchRequest|error {
    return initialFetchRequest(topic, (), position, requested);
}
