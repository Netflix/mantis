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

package io.mantisrx.publish.core;

import io.mantisrx.publish.MantisEventPublisher;
import io.mantisrx.publish.internal.mql.MQLSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;


/**
 * A factory that creates a subscription type based on {@link MQLSubscription ).
 */
public class SubscriptionFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionFactory.class);

    /**
     * Selects the correct implementation of {@link Subscription}.
     *
     * @param id        A string representing the query ids, usually {@code clientId_subscriptionId}.
     * @param criterion The query string
     *
     * @return An Optional instance implementing {@link Subscription} for use with the {@link MantisEventPublisher}, empty Optional if the criterion is invalid.
     */
    public static Optional<Subscription> getSubscription(String id, String criterion) {
        try {
            MQLSubscription mqlSubscription = new MQLSubscription(id, criterion);
            return ofNullable(mqlSubscription);
        } catch (Throwable t) {
            LOG.info("Failed to get Subscription object for {} {}", id, criterion, t);
            return empty();
        }
    }
}
