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

final int MAX_FETCH_REQUEST_CREDIT = 100;

# Tracks positive Salesforce fetch credits for a single sequential
# subscription. This object is internal connector state exposed temporarily to
# allow deterministic unit testing of the delivery bound.
public isolated class FlowController {
    private final int normalCapacity;
    private int outstandingCredit = 0;
    private int bufferedEvents = 0;
    private boolean livenessReserveInUse = false;
    private boolean livenessReserveBuffered = false;

    # Creates a controller with the requested event-count buffer.
    #
    # + bufferSize - maximum normal events awaiting sequential processing
    # + return - a controller or an error when bufferSize is not positive
    public isolated function init(int bufferSize) returns error? {
        if bufferSize <= 0 {
            return error("bufferSize must be greater than zero");
        }
        self.normalCapacity = bufferSize;
    }

    # Reserves the initial positive FetchRequest credit.
    #
    # + return - number of events to request
    public isolated function initialRequest() returns int|error {
        lock {
            if self.outstandingCredit != 0 || self.bufferedEvents != 0 {
                return error("initial request has already been made");
            }
            int initialCredit = self.normalCapacity > MAX_FETCH_REQUEST_CREDIT ? MAX_FETCH_REQUEST_CREDIT : self.normalCapacity;
            self.outstandingCredit = initialCredit;
            return initialCredit;
        }
    }

    # Accounts for events received from Salesforce against previously requested credit.
    #
    # + count - number of received events
    # + return - an error for unexpected or excessive delivery
    public isolated function received(int count) returns error? {
        lock {
            if count <= 0 || count > self.outstandingCredit {
                return error("received events exceed outstanding fetch credit");
            }
            self.outstandingCredit -= count;
            self.bufferedEvents += count;
            if self.livenessReserveInUse && !self.livenessReserveBuffered && count == 1 &&
                    self.outstandingCredit == 0 {
                self.livenessReserveBuffered = true;
            }
        }
    }

    # Uses the one internal reserve slot when a full normal buffer would
    # otherwise leave Salesforce with no outstanding positive request.
    #
    # + return - one for a new positive request, or zero when reserve is unavailable
    public isolated function requestLivenessReserve() returns int|error {
        lock {
            if self.livenessReserveInUse || self.bufferedEvents + self.outstandingCredit < self.normalCapacity {
                return 0;
            }
            self.livenessReserveInUse = true;
            self.outstandingCredit += 1;
            return 1;
        }
    }

    # Releases capacity only after the current event was both handled and
    # durably checkpointed. Callers must identify a liveness-reserve event from
    # their queue metadata; it is never replaced by another request.
    #
    # + livenessReserveEvent - whether the completed event consumed the reserve
    # + return - a positive replacement request, or zero for a reserve event
    public isolated function checkpointed(boolean livenessReserveEvent = false) returns int|error {
        lock {
            if self.bufferedEvents <= 0 {
                return error("no buffered event is available to checkpoint");
            }
            if livenessReserveEvent {
                if !self.livenessReserveBuffered {
                    return error("no liveness reserve event is buffered");
                }
                self.bufferedEvents -= 1;
                self.livenessReserveBuffered = false;
                self.livenessReserveInUse = false;
                return 0;
            }

            self.bufferedEvents -= 1;
            if self.bufferedEvents + self.outstandingCredit >= self.normalCapacity {
                return 0;
            }
            self.outstandingCredit += 1;
            return 1;
        }
    }
}
