/*
 * Copyright 2021 Netflix, Inc.
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

import static com.mantisrx.common.utils.MantisMetricStringConstants.INCOMING;
import static io.mantisrx.server.core.stats.MetricStringConstants.DATA_DROP_METRIC_GROUP;
import static io.mantisrx.server.core.stats.MetricStringConstants.DROP_COUNT;
import static io.mantisrx.server.core.stats.MetricStringConstants.ON_NEXT_COUNT;
import static io.reactivx.mantis.operators.DropOperator.METRIC_GROUP;
import static org.junit.Assert.assertEquals;

import com.netflix.spectator.api.DefaultRegistry;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.common.metrics.spectator.SpectatorRegistryFactory;
import io.reactivx.mantis.operators.DropOperator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataDroppedPayloadSetterTest {
    private static final Logger logger = LoggerFactory.getLogger(DataDroppedPayloadSetterTest.class);

    @Test
    public void testAggregateDropOperatorMetrics() throws Exception {
        SpectatorRegistryFactory.setRegistry(new DefaultRegistry());
        Heartbeat heartbeat = new Heartbeat("job-1", 1, 1, 1);
        DataDroppedPayloadSetter payloadSetter = new DataDroppedPayloadSetter(heartbeat);

        Metrics m = new Metrics.Builder()
                .id(METRIC_GROUP + "_" + INCOMING + "_metric1")
                .addCounter(DropOperator.Counters.dropped.toString())
                .addCounter(DropOperator.Counters.onNext.toString())
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        m.getCounter(DropOperator.Counters.dropped.toString()).increment(1);
        m.getCounter(DropOperator.Counters.onNext.toString()).increment(10);
        m = new Metrics.Builder()
                .id(METRIC_GROUP + "_" + INCOMING + "_metric2")
                .addCounter(DropOperator.Counters.dropped.toString())
                .addCounter(DropOperator.Counters.onNext.toString())
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        m.getCounter(DropOperator.Counters.dropped.toString()).increment(100);
        m.getCounter(DropOperator.Counters.onNext.toString()).increment(1000);

        payloadSetter.setPayload(30);
        m = MetricsRegistry.getInstance().getMetric(new MetricGroupId(DATA_DROP_METRIC_GROUP));
        assertEquals(101L, m.getGauge(DROP_COUNT).value());
        assertEquals(1010, m.getGauge(ON_NEXT_COUNT).value());
    }
}
