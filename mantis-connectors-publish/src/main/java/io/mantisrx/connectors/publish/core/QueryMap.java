/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connectors.publish.core;

import static io.mantisrx.connectors.publish.core.ObjectUtils.checkNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.mantisrx.publish.proto.MantisServerSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryMap.class);

    private final Map<String, String> emptyMap = new HashMap<>(0);

    private final ConcurrentHashMap<String, MantisServerSubscriptionWrapper> subscriptionMap =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<String, MantisServerSubscriptionWrapper>> appToSubscriptionMap =
            new ConcurrentHashMap<>();
    private final String clientIdPrefix;


    QueryMap(String clientIdPrefix) {
        this.clientIdPrefix = clientIdPrefix;
    }

    void registerQuery(String subId, String query, Map<String, String> emptyMap) {
        registerQuery(subId, query, this.emptyMap, false);

    }

    void registerQuery(String subId, String query,
                       Map<String, String> additionalParams,
                       boolean validateQueryAsGroovy) {
        checkNotNull("subscriptionId", subId);
        checkNotNull("query", query);
        Map<String, String> addParams = (additionalParams == null) ? emptyMap : additionalParams;

        subscriptionMap.computeIfAbsent(subId, (s) -> new MantisServerSubscriptionWrapper(addMantisPrefix(subId), query, addParams)).incrementAndGetRefCount();
    }

    boolean deregisterQuery(String subId, String query) {
        MantisServerSubscriptionWrapper subscription = subscriptionMap.computeIfPresent(subId, (k, v) -> {
            v.decrementRefCount();
            return v;
        });

        if (subscription != null) {
            if (subscription.getRefCount() <= 0) {
                LOGGER.info("Subscription ref count is 0 for subscriptionId " + subId + " removing subscription");
                subscriptionMap.remove(subId);
            } else {
                LOGGER.info("Subscription ref count decremented for subscriptionId " + subId);
            }
        } else {
            LOGGER.info("Subscription " + subId + " not found");
        }

        return true;
    }

    public List<MantisServerSubscription> getCurrentSubscriptions() {
        return subscriptionMap.values().stream().map(MantisServerSubscriptionWrapper::getSubscription).collect(Collectors.toList());
    }

    private String addMantisPrefix(String subId) {
        return clientIdPrefix + "_" + subId;
    }

    public static class Builder {
        String prefix = null;

        Builder() {
        }

        Builder withClientIdPrefix(String prefix) {
            checkNotNull("prefix", prefix);
            this.prefix = prefix;
            return this;
        }

        QueryMap build() {
            checkNotNull("prefix", this.prefix);
            return new QueryMap(prefix);
        }
    }

    public static class MantisServerSubscriptionWrapper {
        private final MantisServerSubscription subscription;

        // Used to dedup erroneous subscriptions from client.
        AtomicInteger refCount = new AtomicInteger();

        MantisServerSubscriptionWrapper(String subId,
                                        String query,
                                        Map<String, String> additionalParams) {
            this.subscription = new MantisServerSubscription(subId, query, additionalParams);
        }

        MantisServerSubscription getSubscription() {
            return this.subscription;
        }

        int incrementAndGetRefCount() {
            return refCount.incrementAndGet();
        }

        void decrementRefCount() {
            refCount.decrementAndGet();
        }

        int getRefCount() {
            return refCount.get();
        }

        @Override
        public String toString() {
            return "MantisServerSubscriptionWrapper{"
                    + " subscription=" + subscription
                    + ", refCount=" + refCount
                    + '}';
        }
    }
}
