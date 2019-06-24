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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.mantisrx.publish.proto.MantisServerSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryRegistry {

    public static final String ANY = "ANY";

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRegistry.class);

    private final Map<String, String> emptyMap = new HashMap<>(0);

    private final ConcurrentMap<String, QueryMap> appToSubscriptionMap = new ConcurrentHashMap<>();
    private final String clientIdPrefix;

    private QueryRegistry(String clientIdPrefix) {
        this.clientIdPrefix = clientIdPrefix;
    }

    public void registerQuery(String targetApp, String subId, String query) {
        registerQuery(targetApp, subId, query, this.emptyMap, false);

    }

    public void registerQuery(String targetApp, String subId, String query, Map<String, String> additionalParams, boolean validateQueryAsGroovy) {
        checkNotNull("subscriptionId", subId);
        checkNotNull("query", query);
        checkNotNull("targetAppName", targetApp);
        Map<String, String> addParams = (additionalParams == null) ? emptyMap : additionalParams;

        appToSubscriptionMap.putIfAbsent(targetApp, new QueryMap(clientIdPrefix));

        appToSubscriptionMap.get(targetApp).registerQuery(subId, query, additionalParams, validateQueryAsGroovy);
    }

    public boolean deregisterQuery(String targetApp, String subId, String query) {
        appToSubscriptionMap.computeIfPresent(targetApp, (k, v) -> {
            v.deregisterQuery(subId, query);
            return v;
        });

        return true;
    }

    public List<MantisServerSubscription> getCurrentSubscriptionsForApp(String app) {
        List<MantisServerSubscription> subsForApp = (appToSubscriptionMap.containsKey(app)) ? appToSubscriptionMap.get(app).getCurrentSubscriptions() : new ArrayList<>();
        if (!app.equals(ANY) && appToSubscriptionMap.containsKey(ANY)) {
            subsForApp.addAll(appToSubscriptionMap.get(ANY).getCurrentSubscriptions());
        }

        return subsForApp;
    }

    /**
     * Returns a list of {@link MantisServerSubscription}s.
     *
     * @param queryParams key-value pairs of stream-queries.
     */
    public List<MantisServerSubscription> getCurrentSubscriptions(Map<String, List<String>> queryParams) {
        String app = ANY;

        if (queryParams.containsKey("app")) {
            app = queryParams.get("app").get(0);
        }

        return getCurrentSubscriptionsForApp(app);
    }

    public Map<String, List<MantisServerSubscription>> getAllSubscriptions() {
        Map<String, List<MantisServerSubscription>> allSubMap = new HashMap<>();
        appToSubscriptionMap.forEach((s, q) -> {
            allSubMap.put(s, q.getCurrentSubscriptions());
        });

        return allSubMap;
    }

    private String addMantisPrefix(String subId) {
        return clientIdPrefix + "_" + subId;
    }

    public static class Builder {
        private String prefix = null;

        public Builder() {
        }

        public Builder withClientIdPrefix(String prefix) {
            checkNotNull("prefix", prefix);
            this.prefix = prefix;
            return this;
        }

        public QueryRegistry build() {
            checkNotNull("prefix", this.prefix);
            return new QueryRegistry(prefix);
        }
    }
}
