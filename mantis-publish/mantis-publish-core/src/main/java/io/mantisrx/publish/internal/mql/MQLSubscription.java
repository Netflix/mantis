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

package io.mantisrx.publish.internal.mql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.mantisrx.mql.jvm.core.Query;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.core.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link Subscription} based on the Mantis Query Language (MQL).
 */
public class MQLSubscription implements Subscription, Comparable {

    private static final Logger LOG = LoggerFactory.getLogger(MQLSubscription.class);
    protected final Query query;
    private AtomicBoolean matcherErrorLoggingEnabled;
    private AtomicBoolean projectorErrorLoggingEnabled;

    private ConcurrentHashMap<
            HashSet<Query>,
            Function<
                    Map<String, Object>,
                    Map<String, Object>>> superSetProjectorCache;

    public MQLSubscription(String subId, String criterion) {
        this.superSetProjectorCache = new ConcurrentHashMap<>();
        this.matcherErrorLoggingEnabled = new AtomicBoolean(true);
        this.projectorErrorLoggingEnabled = new AtomicBoolean(true);
        this.query = MQL.query(subId, MQL.preprocess(criterion));
    }

    private Map<String, Object> projectSuperSet(
            Collection<Query> queries, Map<String, Object> datum) {

        Function<Map<String, Object>, Map<String, Object>> superSetProjector =
                superSetProjectorCache.computeIfAbsent(
                        new HashSet<>(queries), MQL::makeSupersetProjector);

        return superSetProjector.apply(datum);
    }

    public Query getQuery() {
        return query;
    }

    public String getRawQuery() {
        return this.query.getRawQuery();
    }

    public String getSubscriptionId() {
        return this.query.getSubscriptionId();
    }

    public Event projectSuperset(List<Subscription> subscriptions, Event event) {
        List<Query> queries = new ArrayList<>(subscriptions.size());
        for (Subscription sub : subscriptions) {
            queries.add(sub.getQuery());
        }

        try {
            return new Event(projectSuperSet(queries, event.getMap()));
        } catch (Exception e) {
            if (projectorErrorLoggingEnabled.get()) {
                LOG.error("MQL projector produced an exception on queries: {}\ndatum: {}.",
                        queries, event.getMap());
                projectorErrorLoggingEnabled.set(false);
            }

            Event error = new Event();
            error.set("message", e.getMessage());
            error.set("queries",
                    queries.stream()
                            .map(Query::getRawQuery)
                            .collect(Collectors.joining(", ")));

            return error;
        }
    }

    public boolean matches(Event event) {
        try {
            return this.query.matches(event.getMap());
        } catch (Exception ex) {
            if (matcherErrorLoggingEnabled.get()) {
                LOG.error("MQL matcher produced an exception on query: {}\ndatum: {}.",
                        this.query.getRawQuery(), event.getMap());
                matcherErrorLoggingEnabled.set(false);
            }

            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public List<String> getSubjects() {
        return this.query.getSubjects();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((this.query == null) ? 0 : this.query.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MQLSubscription other = (MQLSubscription) obj;
        if (this.query == null) {
            if (other.query != null)
                return false;
        } else if (!query.equals(other.query))
            return false;
        return true;
    }

    @Override
    public int compareTo(final Object o) {
        MQLSubscription other = (MQLSubscription) o;
        if (this.equals(other)) {
            return 0;
        }
        if (other != null) {
            if (this.query.equals(other.query)) {
                return 0;
            } else {
                int result = this.getSubscriptionId().compareTo(other.getSubscriptionId());
                result = result == 0 ? this.getRawQuery().compareTo(other.getRawQuery()) : result;
                // compareTo should confirm with equals,
                // so return non-zero result if the subIds/query are same for the two queries
                return result == 0 ? -1 : result;
            }
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "MQLSubscription [subId=" + this.query.getSubscriptionId() +
                ", criterion=" + this.query.getRawQuery() + "]";
    }
}
