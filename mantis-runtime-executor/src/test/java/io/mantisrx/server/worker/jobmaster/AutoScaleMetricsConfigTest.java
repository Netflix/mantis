/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.server.worker.jobmaster;

import static io.reactivex.mantis.network.push.PushServerSse.DROPPED_COUNTER_METRIC_NAME;
import static io.reactivex.mantis.network.push.PushServerSse.PROCESSED_COUNTER_METRIC_NAME;
import static org.junit.Assert.*;

import io.mantisrx.shaded.com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.junit.Test;

public class AutoScaleMetricsConfigTest {
    @Test
    public void testGenerateSourceJobMetricGroups() {
        AutoScaleMetricsConfig config = new AutoScaleMetricsConfig();

        Set<String> groups = config.generateSourceJobMetricGroups(ImmutableSet.of("clientId1", "client-id-2"));
        Set<String> expected = ImmutableSet.of("PushServerSse:clientId=clientId1:*", "PushServerSse:clientId=client-id-2:*",
                "ServerSentEventRequestHandler:clientId=clientId1:*", "ServerSentEventRequestHandler:clientId=client-id-2:*");
        assertEquals(expected, groups);
    }

    @Test
    public void testGetAggregationAlgoForSourceJobMetrics() throws Exception {
        AutoScaleMetricsConfig config = new AutoScaleMetricsConfig();

        AutoScaleMetricsConfig.AggregationAlgo aglo = config.getAggregationAlgo(
                "ServerSentEventRequestHandler:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", DROPPED_COUNTER_METRIC_NAME);
        assertEquals(AutoScaleMetricsConfig.AggregationAlgo.MAX, aglo);
        assertTrue(config.isSourceJobDropMetric("ServerSentEventRequestHandler:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", DROPPED_COUNTER_METRIC_NAME));

        aglo = config.getAggregationAlgo(
                "PushServerSse:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", DROPPED_COUNTER_METRIC_NAME);
        assertEquals(AutoScaleMetricsConfig.AggregationAlgo.MAX, aglo);

        aglo = config.getAggregationAlgo(
                "PushServerSse:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", PROCESSED_COUNTER_METRIC_NAME);
        assertEquals(AutoScaleMetricsConfig.AggregationAlgo.AVERAGE, aglo);

        aglo = config.getAggregationAlgo(
                "ABCServerSentEventRequestHandler:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", DROPPED_COUNTER_METRIC_NAME);
        assertEquals(AutoScaleMetricsConfig.AggregationAlgo.AVERAGE, aglo);

        aglo = config.getAggregationAlgo(
                "PushServerSse:clientId=ABC:DEF", DROPPED_COUNTER_METRIC_NAME);
        assertEquals(AutoScaleMetricsConfig.AggregationAlgo.MAX, aglo);
    }

    @Test
    public void testAddSourceJobDropMetrics() {
        AutoScaleMetricsConfig config = new AutoScaleMetricsConfig();
        config.addSourceJobDropMetrics("myDropGroup1:clientId=_CLIENT_ID_:*::myDropCounter::MAX");

        AutoScaleMetricsConfig.AggregationAlgo aglo = config.getAggregationAlgo(
                "myDropGroup1:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", "myDropCounter");
        assertEquals(AutoScaleMetricsConfig.AggregationAlgo.MAX, aglo);
        assertTrue(config.isSourceJobDropMetric("myDropGroup1:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", "myDropCounter"));
        assertFalse(config.isSourceJobDropMetric("ABCmyDropGroup1:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", "myDropCounter"));
        assertTrue(config.isSourceJobDropMetric("ServerSentEventRequestHandler:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", DROPPED_COUNTER_METRIC_NAME));
    }

    @Test
    public void testAddSourceJobDropMetricsThrowsException() {
        AutoScaleMetricsConfig config = new AutoScaleMetricsConfig();
        try {
            config.addSourceJobDropMetrics("InvalidMetricFormat");
            fail();
        } catch (Exception ex) {
            // pass
        }
    }

    @Test
    public void testAddSourceJobDropMetricsEmptyString() {
        AutoScaleMetricsConfig config = new AutoScaleMetricsConfig();
        config.addSourceJobDropMetrics(null);
        config.addSourceJobDropMetrics("");

        assertTrue(config.isSourceJobDropMetric("ServerSentEventRequestHandler:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", DROPPED_COUNTER_METRIC_NAME));
    }
}
