package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.shaded.com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Set;

import static io.reactivex.mantis.network.push.PushServerSse.DROPPED_COUNTER_METRIC_NAME;
import static io.reactivex.mantis.network.push.PushServerSse.PROCESSED_COUNTER_METRIC_NAME;
import static org.junit.Assert.assertEquals;

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
    public void testgetAggregationAlgoForSourceJobMetrics() throws Exception {
        AutoScaleMetricsConfig config = new AutoScaleMetricsConfig();

        AutoScaleMetricsConfig.AggregationAlgo aglo = config.getAggregationAlgo(
                "ServerSentEventRequestHandler:clientId=RavenConnectorJob-1657357:sockAddr=/100.87.51.222", DROPPED_COUNTER_METRIC_NAME);
        assertEquals(AutoScaleMetricsConfig.AggregationAlgo.MAX, aglo);

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
                "PushServerSse:ABC:DEF", DROPPED_COUNTER_METRIC_NAME);
        assertEquals(AutoScaleMetricsConfig.AggregationAlgo.MAX, aglo);
    }
}
