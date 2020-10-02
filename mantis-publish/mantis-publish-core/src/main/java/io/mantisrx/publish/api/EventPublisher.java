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

import java.util.concurrent.CompletionStage;

import io.mantisrx.publish.core.Subscription;


public interface EventPublisher {

    /**
     * Publishes an event on the given stream.
     *
     * @param streamName name of the stream to publish the event to
     * @param event      event data to publish to Mantis
     * @return {@link CompletionStage<PublishStatus>} status of publishing the message
     */
    CompletionStage<PublishStatus> publish(String streamName, Event event);

    /**
     * Publishes an event on the {@value StreamType#DEFAULT_EVENT_STREAM} stream.
     * <p>To publish an Event, use the following code:
     * <pre>   {@code
     *   eventPublisher.publish(new Event(attr))
     *      .whenComplete((s, t) -> {
     *     if (t != null) {
     *         LOG.error("caught exception processing event", t);
     *     } else {
     *         switch (s.getStatus()) {
     *             case SENDING:
     *             case SENT:
     *                 // success
     *                 break;
     *             case PRECONDITION_FAILED:
     *                 // message was skipped due to client being disabled, no active MQL subs etc,increment a counter for visibility
     *                 break;
     *             case FAILED:
     *                 // error sending message, increment a counter and raise an alarm if too many failures
     *                 LOG.error("failed to send event", s);
     *                 break;
     *             default:
     *         }
     *     }
     * });</pre>
     * @param event event data to publish to Mantis
     * @return {@link CompletionStage<PublishStatus>} status of publishing the message
     */
    CompletionStage<PublishStatus> publish(Event event);

    /**
     * Returns whether or not this event publisher has active {@link Subscription}s.
     * <p>
     * This method is useful for checking for the existence of a stream before
     * calling {@link EventPublisher#publish(Event)} to avoid the performance penalty when there
     * are no active subscriptions for the stream.
     *
     * @param streamName name of the event stream
     */
    boolean hasSubscriptions(String streamName);
}
