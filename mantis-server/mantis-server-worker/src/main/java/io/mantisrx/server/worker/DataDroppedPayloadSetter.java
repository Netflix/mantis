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

package io.mantisrx.server.worker;

import static com.mantisrx.common.utils.MantisMetricStringConstants.DROP_OPERATOR_INCOMING_METRIC_GROUP;
import static io.mantisrx.server.core.stats.MetricStringConstants.DATA_DROP_METRIC_GROUP;
import static io.mantisrx.server.core.stats.MetricStringConstants.DROP_COUNT;
import static io.mantisrx.server.core.stats.MetricStringConstants.ON_NEXT_COUNT;

import java.util.Collection;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.core.StatusPayloads;
import io.reactivx.mantis.operators.DropOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class DataDroppedPayloadSetter {

    private static final String metricNamePrefix = DROP_OPERATOR_INCOMING_METRIC_GROUP;
    private static final Logger logger = LoggerFactory.getLogger(DataDroppedPayloadSetter.class);
    private static final double bigIncreaseThreshold = 0.05;
    private final Heartbeat heartbeat;
    private final AtomicLong prevDroppedCount = new AtomicLong(-1L);
    private final AtomicLong prevOnNextCount = new AtomicLong(0L);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledThreadPoolExecutor executor;

    private final Gauge dropCountGauge;
    private final Gauge onNextCountGauge;


    DataDroppedPayloadSetter(Heartbeat heartbeat) {
        this.heartbeat = heartbeat;
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        executor = new ScheduledThreadPoolExecutor(1);
        Metrics m = new Metrics.Builder()
                .name(DATA_DROP_METRIC_GROUP)
                .addGauge(DROP_COUNT)
                .addGauge(ON_NEXT_COUNT)
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        dropCountGauge = m.getGauge(DROP_COUNT);
        onNextCountGauge = m.getGauge(ON_NEXT_COUNT);
    }

    private void setPayload(final long intervalSecs) {
        final Collection<Metrics> metrics = MetricsRegistry.getInstance().getMetrics(metricNamePrefix);
        long totalDropped = 0L;
        long totalOnNext = 0L;
        long delay = intervalSecs;
        try {
            if (metrics != null && !metrics.isEmpty()) {
                //logger.info("Got " + metrics.size() + " metrics for DropOperator");
                for (Metrics m : metrics) {
                    final Counter dropped = m.getCounter("" + DropOperator.Counters.dropped);
                    final Counter onNext = m.getCounter("" + DropOperator.Counters.onNext);
                    if (dropped != null)
                        totalDropped += dropped.value();
                    else
                        logger.warn("Unexpected to get null dropped counter for metric " + m.getMetricGroupId().id());
                    if (onNext != null)
                        totalOnNext += onNext.value();
                    else
                        logger.warn("Unexpected to get null onNext counter for metric " + m.getMetricGroupId().id());
                }
                if (!isBigChange(prevDroppedCount.get(), totalDropped))
                    delay *= 2;
                final StatusPayloads.DataDropCounts dataDrop = new StatusPayloads.DataDropCounts(totalOnNext - prevOnNextCount.get(),
                        totalDropped - prevDroppedCount.get());
                try {
                    heartbeat.addSingleUsePayload("" + StatusPayloads.Type.IncomingDataDrop, objectMapper.writeValueAsString(dataDrop));
                } catch (JsonProcessingException e) {
                    logger.warn("Error writing json for dataDrop payload: " + e.getMessage());
                }
                dropCountGauge.set(dataDrop.getDroppedCount());
                onNextCountGauge.set(dataDrop.getOnNextCount());

                prevDroppedCount.set(totalDropped);
                prevOnNextCount.set(totalOnNext);
            } else
                logger.warn("Got no metrics from DropOperator");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                setPayload(intervalSecs);
            }
        }, delay, TimeUnit.SECONDS);
    }

    private boolean isBigChange(long prevTotalDropped, long totalDropped) {
        if (prevTotalDropped < 0L)
            return true;
        if (prevTotalDropped == 0)
            return totalDropped != 0;
        if (totalDropped == 0)
            return true;
        return (double) Math.abs(totalDropped - prevTotalDropped) / (double) totalDropped > bigIncreaseThreshold;
    }

    void start(final long intervalSecs) {
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                setPayload(intervalSecs);
            }
        }, intervalSecs, TimeUnit.SECONDS);
    }
}
