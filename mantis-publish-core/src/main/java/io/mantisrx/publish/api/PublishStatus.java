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

public enum PublishStatus {
    /**
     * Successfully enqueued the message for further MQL processing.
     * This only indicates enqueue success and does not indicate the message was successfully MQL processed and sent to Mantis
     */
    ENQUEUED,

    // Skipped messages due to no active subscriptions or publishing being disabled from config
    /**
     * Message was not enqueued since there are no active MQL subscriptions for the streamName
     */
    SKIPPED_NO_SUBSCRIPTIONS,
    /**
     * Message was not enqueued since the Mantis Publish client is not enabled in configuration
     */
    SKIPPED_CLIENT_NOT_ENABLED,

    // Messages dropped due to failures like burst in incoming traffic, too many stream queues created, transport issues
    /**
     * Message enqueue failed due to stream queue being full
     */
    FAILED_QUEUE_FULL,
    /**
     * Message enqueue failed as the stream could not be registered, this could happen if max number of streams allowed is exceeded
     */
    FAILED_STREAM_NOT_REGISTERED
}
