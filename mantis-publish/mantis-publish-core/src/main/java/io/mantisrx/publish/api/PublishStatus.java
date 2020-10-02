/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.publish.api;

/**
 * Indicates the result of publishing an Event to Mantis.
 */
public enum PublishStatus {
    /**
     * Successfully enqueued the message for further MQL processing.
     * This only indicates enqueue success and does not indicate the message was successfully MQL processed and sent to Mantis
     */
    ENQUEUED(Status.SENDING),

    // Skipped events due to precondition failures like no active subscriptions, publishing not enabled in configuration
    /**
     * Event was not enqueued since there are no active MQL subscriptions for the streamName
     */
    SKIPPED_NO_SUBSCRIPTIONS(Status.PRECONDITION_FAILED),
    /**
     * Event was not enqueued since the Mantis Publish client is not enabled in configuration
     */
    SKIPPED_CLIENT_NOT_ENABLED(Status.PRECONDITION_FAILED),

    /**
     * Event was not enqueued since the event was invalid (either null or empty)
     */
    SKIPPED_INVALID_EVENT(Status.PRECONDITION_FAILED),

    // Events dropped due to failures like burst in incoming traffic, too many stream queues created, transport issues
    /**
     * Event enqueue failed due to stream queue being full
     */
    FAILED_QUEUE_FULL(Status.FAILED),
    /**
     * Event enqueue failed as the stream could not be registered, this could happen if max number of streams allowed per config is exceeded
     */
    FAILED_STREAM_NOT_REGISTERED(Status.FAILED),
    /**
     * Event could not be sent as the Mantis Worker Info is unavailable for configured Mantis Job Cluster
     */
    FAILED_WORKER_INFO_UNAVAILABLE(Status.FAILED),
    /**
     * Event processing failed to project superset
     */
    FAILED_SUPERSET_PROJECTION(Status.FAILED);

    public enum Status {
        /**
         * Indicates the event was skipped as expected due to a precondition failure
         */
        PRECONDITION_FAILED,
        /**
         * Indicates the event is being sent (considered a successful send until {@link Status#SENT} is implemented).
         * Clients should consider either a {@link Status#SENDING} or {@link Status#SENT} status as a Success to stay forward compatible.
         */
        SENDING,
        /**
         * Not currently implemented but in future would indicate an event was successfully sent over the network to Mantis for further propagation
         */
        SENT,
        /**
         * Failed to send the event
         */
        FAILED
    }

    private final Status status;

    PublishStatus(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return super.toString() + "(" + status + ')';
    }
}
