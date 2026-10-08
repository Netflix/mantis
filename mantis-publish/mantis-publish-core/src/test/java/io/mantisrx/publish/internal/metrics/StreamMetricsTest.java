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

package io.mantisrx.publish.internal.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DefaultRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class StreamMetricsTest {

    private static final String STREAM_NAME = "testStream";

    private DefaultRegistry registry;
    private StreamMetrics streamMetrics;

    @BeforeEach
    void setUp() {
        registry = new DefaultRegistry();
        streamMetrics = new StreamMetrics(registry, STREAM_NAME);
    }

    @Test
    void testGetMantisEventsFilteredCounterIsRegisteredWithExpectedTags() {
        Counter counter = streamMetrics.getMantisEventsFilteredCounter("sub1");
        assertNotNull(counter);

        counter.increment();

        Counter registeredCounter = registry.counter(
                registry.createId("mantisEventsFiltered")
                        .withTag("stream", STREAM_NAME)
                        .withTag("subscriptionId", "sub1"));
        assertEquals(1, registeredCounter.count());
    }

    @Test
    void testGetMantisEventsFilteredCounterCachesInstancePerSubscriptionId() {
        Counter first = streamMetrics.getMantisEventsFilteredCounter("sub1");
        Counter second = streamMetrics.getMantisEventsFilteredCounter("sub1");

        assertSame(first, second);

        first.increment();
        second.increment();

        assertEquals(2, first.count());
    }

    @Test
    void testGetMantisEventsFilteredCounterReturnsDistinctCountersPerSubscriptionId() {
        Counter subOneCounter = streamMetrics.getMantisEventsFilteredCounter("sub1");
        Counter subTwoCounter = streamMetrics.getMantisEventsFilteredCounter("sub2");

        assertNotSame(subOneCounter, subTwoCounter);

        subOneCounter.increment();

        assertEquals(1, subOneCounter.count());
        assertEquals(0, subTwoCounter.count());
    }

    @Test
    void testSubscriptionIdsAboveCardinalityLimitCollapseToOtherBucket() {
        for (int i = 0; i < 50; i++) {
            streamMetrics.getMantisEventsFilteredCounter("sub" + i).increment();
        }

        streamMetrics.getMantisEventsFilteredCounter("overflow-sub").increment();

        Counter otherBucket = registry.counter(
                registry.createId("mantisEventsFiltered")
                        .withTag("stream", STREAM_NAME)
                        .withTag("subscriptionId", "--others--"));
        assertEquals(1, otherBucket.count(),
                "subscriptionIds beyond the cardinality limit must be collapsed to '--others--'");
    }

    @Test
    void testMultipleOverflowIdsAccumulateInSameBucket() {
        for (int i = 0; i < 50; i++) {
            streamMetrics.getMantisEventsFilteredCounter("sub" + i).increment();
        }

        streamMetrics.getMantisEventsFilteredCounter("overflow-1").increment();
        streamMetrics.getMantisEventsFilteredCounter("overflow-2").increment();

        Counter otherBucket = registry.counter(
                registry.createId("mantisEventsFiltered")
                        .withTag("stream", STREAM_NAME)
                        .withTag("subscriptionId", "--others--"));
        assertEquals(2, otherBucket.count(),
                "all overflow subscriptionIds must accumulate in the single '--others--' bucket");
    }
}
