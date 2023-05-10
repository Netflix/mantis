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

package io.mantisrx.common.metrics;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.common.metrics.spectator.CounterImpl;
import io.mantisrx.common.metrics.spectator.GaugeImpl;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.common.metrics.spectator.MetricId;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.mantisrx.common.metrics.spectator.TimerImpl;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Metrics {

    private static final Logger logger = LoggerFactory.getLogger(Metrics.class);

    private final Builder builder;
    private MetricGroupId metricGroup;
    private Map<MetricId, Counter> counters = new HashMap<>();
    private Map<MetricId, Gauge> gauges = new HashMap<>();
    private Map<MetricId, Timer> timers = new HashMap<>();

    public Metrics(Builder builder) {
        this.builder = builder;
        this.metricGroup = builder.metricGroup;

        // create spectator counters
        if (builder.counterIds != null && builder.counterIds.size() > 0) {
            for (MetricId id : builder.counterIds) {
                logger.debug("creating spectator counter for {}", id);
                counters.put(id, new CounterImpl(id, builder.registry));
            }
        }

        // create gauges
        if (builder.callbackGauges != null && builder.callbackGauges.size() > 0) {
            for (Gauge gauge : builder.callbackGauges) {
                gauges.put(gauge.id(), gauge);
            }
        }

        // create spectator gauges
        if (builder.gaugeIds != null && builder.gaugeIds.size() > 0) {
            for (MetricId gaugeId : builder.gaugeIds) {
                logger.debug("creating spectator gauge for {}", gaugeId);
                gauges.put(gaugeId, new GaugeImpl(gaugeId, builder.registry));
            }
        }

        if (builder.timerIds != null && builder.timerIds.size() > 0) {
            for (MetricId id : builder.timerIds) {
                logger.debug("creating spectator timer for {}", id);
                timers.put(id, new TimerImpl(id, builder.registry));
            }
        }
    }

    /**
     * use {@link #getMetricGroupId()} instead
     *
     * @return
     */
    @Deprecated
    public String getMetricGroup() {
        return metricGroup.name();
    }

    public MetricGroupId getMetricGroupId() {
        return metricGroup;
    }

    @Override
    public String toString() {
        return "Metrics{" +
            "metricGroup=" + metricGroup +
            ", counters=" + counters.keySet() +
            ", gauges=" + gauges.keySet() +
            ", timers=" + timers.keySet() +
            '}';
    }

    public Counter getCounter(String metricName) {
        Counter counter = counters.get(new MetricId(metricGroup.name(), metricName, metricGroup.tags()));
        if (counter == null) {
            throw new RuntimeException("No counter registered for metricGroup: " + metricGroup + " with metricName: " + metricName);
        }
        return counter;
    }

    public Gauge getGauge(String metricName) {
        Gauge gauge = gauges.get(new MetricId(metricGroup.name(), metricName, metricGroup.tags()));
        if (gauge == null) {
            // For backwards compat
            final Gauge legacyGauge = gauges.get(new MetricId(GaugeCallback.LEGACY_GAUGE_CALLBACK_METRICGROUP, metricName));
            if (legacyGauge == null) {
                throw new RuntimeException("No gauge registered for metricGroup: " + metricGroup + " with name: " + metricName);
            } else {
                return legacyGauge;
            }
        }
        return gauge;
    }

    public Timer getTimer(String metricName) {
        Timer timer = timers.get(new MetricId(metricGroup.name(), metricName, metricGroup.tags()));
        if (timer == null) {
            throw new RuntimeException("No timer registered for metriGroup: " + metricGroup + " with name: " + metricName);
        }
        return timer;
    }

    public Counter getCounter(final String metricName, final Tag... tags) {
        Counter counter = counters.get(new MetricId(metricGroup.name(), metricName, tags));
        if (counter == null) {
            throw new RuntimeException("No counter registered for metricGroup: " + metricGroup + " with metricName: " + metricName);
        }
        return counter;
    }

    public Gauge getGauge(final String metricName, final Tag... tags) {
        Gauge gauge = gauges.get(new MetricId(metricGroup.name(), metricName, tags));
        if (gauge == null) {
            throw new RuntimeException("No gauge registered for metricGroup: " + this.metricGroup + " with metricName: " + metricName);
        }
        return gauge;
    }

    public Timer getTimer(final String metricName, final Tag... tags) {
        Timer timer = timers.get(new MetricId(metricGroup.name(), metricName, tags));
        if (timer == null) {
            throw new RuntimeException("No timer registered for metricGroup: " + this.metricGroup + " with metricName: " + metricName);
        }
        return timer;
    }

    /**
     * @return Note, point in time copy, if counters are add after copy they
     * will not be reflected in this data structure.
     */
    public Map<MetricId, Counter> counters() {
        return Collections.unmodifiableMap(counters);
    }

    /**
     * @return Note, point in time copy, if gauge are add after copy they
     * will not be reflected in this data structure.
     */
    public Map<MetricId, Gauge> gauges() {
        return Collections.unmodifiableMap(gauges);
    }

    public Map<MetricId, Timer> timers() {
        return Collections.unmodifiableMap(timers);
    }

    public static class Builder {

        private final Registry registry;
        private final Set<Gauge> callbackGauges = new HashSet<>();
        private final Set<MetricId> counterIds = new HashSet<>();
        private final Set<MetricId> gaugeIds = new HashSet<>();
        private final Set<MetricId> timerIds = new HashSet<>();
        private MetricGroupId metricGroup;

        public Builder() {
            this(SpectatorRegistryFactory.getRegistry());
        }

        public Builder(final Registry registry) {
            this.registry = registry;
        }

        public Builder name(final String metricGroup) {
            this.metricGroup = new MetricGroupId(metricGroup);
            return this;
        }

        public Builder id(final MetricGroupId metricGroup) {
            this.metricGroup = metricGroup;
            return this;
        }

        public Builder id(final String metricGroup, final Collection<Tag> groupTags) {
            this.metricGroup = new MetricGroupId(metricGroup, groupTags);
            return this;
        }

        public Builder id(final String metricGroup, final Tag... groupTags) {
            this.metricGroup = new MetricGroupId(metricGroup, groupTags);
            return this;
        }

        public Builder addCounter(final String metricName) {
            Preconditions.checkNotNull(metricGroup, "set metric group id with id(String, Tag...) before adding Counter");
            counterIds.add(new MetricId(metricGroup.name(), metricName, metricGroup.tags()));
            return this;
        }

        @Deprecated
        public Builder addCounter(final String metricName, final Iterable<Tag> overrideGroupTags) {
            Preconditions.checkNotNull(metricGroup, "set metric group id with id(String, Tag...) before adding Counter");
            counterIds.add(new MetricId(metricGroup.name(), metricName, overrideGroupTags));
            return this;
        }

        @Deprecated
        public Builder addCounter(final String metricName, final Tag... overrideGroupTags) {
            Preconditions.checkNotNull(metricGroup, "set metric group id with id(String, Tag...) before adding Counter");
            counterIds.add(new MetricId(metricGroup.name(), metricName, overrideGroupTags));
            return this;
        }

        public Builder addGauge(final String metricName) {
            Preconditions.checkNotNull(metricGroup, "set metric group id with id(String, Tag...) before adding Gauge");
            gaugeIds.add(new MetricId(metricGroup.name(), metricName, metricGroup.tags()));
            return this;
        }

        @Deprecated
        public Builder addGauge(final String metricName, final Iterable<Tag> overrideGroupTags) {
            Preconditions.checkNotNull(metricGroup, "set metric group id with id(String, Tag...) before adding Gauge");
            gaugeIds.add(new MetricId(metricGroup.name(), metricName, overrideGroupTags));
            return this;
        }

        @Deprecated
        public Builder addGauge(final String metricName, final Tag... overrideGroupTags) {
            Preconditions.checkNotNull(metricGroup, "set metric group id with id(String, Tag...) before adding Gauge");
            gaugeIds.add(new MetricId(metricGroup.name(), metricName, overrideGroupTags));
            return this;
        }

        public Builder addGauge(final Gauge callbackGauge) {
            Preconditions.checkNotNull(metricGroup, "set metric group id with id(String, Tag...) before adding Gauge");
            callbackGauges.add(callbackGauge);
            return this;
        }

        public Builder addTimer(final String metricName) {
            Preconditions.checkNotNull(metricGroup, "set metric group id with id(String, Tag...) before adding Timer");
            timerIds.add(new MetricId(metricGroup.name(), metricName, metricGroup.tags()));
            return this;
        }

        public Metrics build() {
            if (metricGroup == null || metricGroup.name().length() == 0) {
                throw new IllegalArgumentException("metricGroup must be specified for metrics");
            }
            return new Metrics(this);
        }
    }
}
