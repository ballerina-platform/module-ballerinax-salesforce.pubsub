// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.

enum DeliveryState {
    IDLE,
    HANDLING,
    CHECKPOINTING,
    RECOVERY_REQUIRED
}

# Serializes one topic's handler, checkpoint, and capacity progression. A
# listener must move it to `IDLE` only after the ReplayStore has accepted the
# exact replay ID for the event.
public isolated class DeliveryGate {
    private DeliveryState state = IDLE;

    # Starts delivery of one queued event.
    #
    # + return - an error when another event is still unresolved
    public isolated function beginEvent() returns error? {
        lock {
            if self.state != IDLE {
                return error("a prior event is unresolved");
            }
            self.state = HANDLING;
        }
    }

    # Records a successful handler return and permits the replay save.
    #
    # + return - an error when no handler invocation is active
    public isolated function handlerSucceeded() returns error? {
        lock {
            if self.state != HANDLING {
                return error("no handler result is awaiting checkpoint");
            }
            self.state = CHECKPOINTING;
        }
    }

    # Marks a failed handler as unresolved until the listener's retry or
    # terminal-recovery path takes ownership.
    public isolated function handlerFailed() {
        lock {
            if self.state == HANDLING {
                self.state = RECOVERY_REQUIRED;
            }
        }
    }

    # Re-enters handler execution for the same unresolved event. This does not
    # make capacity available for later buffered events.
    #
    # + return - an error when there is no failed event awaiting retry
    public isolated function retryHandler() returns error? {
        lock {
            if self.state != RECOVERY_REQUIRED {
                return error("no failed event is awaiting retry");
            }
            self.state = HANDLING;
        }
    }

    # Releases normal delivery only after a successful replay save.
    #
    # + return - an error when no replay save is pending
    public isolated function checkpointSaved() returns error? {
        lock {
            if self.state != CHECKPOINTING {
                return error("no checkpoint save is pending");
            }
            self.state = IDLE;
        }
    }

    # Prevents later work or a keepalive cursor from passing an unresolved
    # checkpoint. The caller must recover or stop the subscription.
    public isolated function checkpointFailed() {
        lock {
            if self.state == CHECKPOINTING {
                self.state = RECOVERY_REQUIRED;
            }
        }
    }

    # Returns whether the next queued event may start.
    #
    # + return - true only when no event is unresolved
    public isolated function canDeliverNext() returns boolean {
        lock {
            return self.state == IDLE;
        }
    }

    # Returns whether the topic can ask Salesforce for replacement capacity.
    #
    # + return - true only when no event is unresolved
    public isolated function canReplenish() returns boolean {
        return self.canDeliverNext();
    }

    # Returns whether a keepalive replay ID may be made durable.
    #
    # + return - true only when no preceding event is unresolved
    public isolated function canCheckpointKeepalive() returns boolean {
        return self.canDeliverNext();
    }
}
