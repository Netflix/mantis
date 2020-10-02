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

import java.util.List;

import io.mantisrx.mql.jvm.core.Query;
import io.mantisrx.publish.internal.mql.MQLSubscription;
import io.mantisrx.publish.api.Event;


public interface Subscription {

    /**
     * See {@link MQLSubscription#matches(Event)}.
     */
    boolean matches(Event event) throws Exception;

    /**
     * See {@link MQLSubscription#projectSuperset(List, Event)}.
     */
    Event projectSuperset(List<Subscription> subscriptions, Event event);

    /**
     * See {@link MQLSubscription#getQuery()}.
     */
    Query getQuery();

    /**
     * See {@link MQLSubscription#getSubscriptionId()}.
     */
    String getSubscriptionId();

    /**
     * See {@link MQLSubscription#getRawQuery()}.
     */
    String getRawQuery();

    /**
     * See {@link MQLSubscription#getSubjects()}.
     */
    List<String> getSubjects();
}
