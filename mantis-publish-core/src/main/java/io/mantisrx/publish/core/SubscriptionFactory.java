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


/**
 * A factory that creates a subscription type based on {@link MQLSubscription ).
 */
public class SubscriptionFactory {

    /**
     * Selects the correct implementation of {@link Subscription}.
     *
     * @param id        A string representing the query ids, usually {@code clientId_subscriptionId}.
     * @param criterion The query string
     *
     * @return An instance implementing {@link Subscription} for use with the {@link MantisEventPublisher}.
     */
    public static Subscription getSubscription(String id, String criterion) {
        return new MQLSubscription(id, criterion);
    }
}
