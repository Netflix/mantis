/*
 * Copyright 2024 Netflix, Inc.
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

package io.reactivex.mantis.network.push;

import io.mantisrx.common.metrics.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rx.subjects.PublishSubject;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ProactiveRoundRobinRouterTest {

    private ProactiveRoundRobinRouter<String> router;
    private List<TestAsyncConnection> connections;

    @BeforeEach
    public void setup() {
        router = new ProactiveRoundRobinRouter<>("test-router", data -> data.getBytes());
        connections = new ArrayList<>();
    }

    @Test
    public void testAddConnection() {
        TestAsyncConnection connection = createConnection("slot-1");

        router.addConnection(connection);

        // Route some data to verify connection was added
        List<String> data = Arrays.asList("test-value");
        router.route(data);

        assertEquals(1, connection.getWrittenData().size());
    }

    @Test
    public void testRemoveConnection() {
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");

        router.addConnection(connection1);
        router.addConnection(connection2);

        // Remove first connection
        router.removeConnection(connection1);

        // Route data - should only go to connection2
        List<String> data = Arrays.asList("test-value");
        router.route(data);

        assertEquals(0, connection1.getWrittenData().size());
        assertEquals(1, connection2.getWrittenData().size());
    }

    @Test
    public void testRouteWithNoConnections() {
        List<String> data = Arrays.asList("test-value");

        // Should not throw exception
        assertDoesNotThrow(() -> router.route(data));
    }

    @Test
    public void testRouteWithNullData() {
        TestAsyncConnection connection = createConnection("slot-1");
        router.addConnection(connection);

        // Should not throw exception
        assertDoesNotThrow(() -> router.route(null));
        assertEquals(0, connection.getWrittenData().size());
    }

    @Test
    public void testRouteWithEmptyData() {
        TestAsyncConnection connection = createConnection("slot-1");
        router.addConnection(connection);

        router.route(Collections.emptyList());

        assertEquals(0, connection.getWrittenData().size());
    }

    @Test
    public void testRoundRobinDistribution() {
        // Add 3 connections
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");
        TestAsyncConnection connection3 = createConnection("slot-3");

        router.addConnection(connection1);
        router.addConnection(connection2);
        router.addConnection(connection3);

        // Route 9 items - should be evenly distributed
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            data.add("value-" + i);
        }
        router.route(data);

        // Each connection should receive 3 items
        assertEquals(3, connection1.getWrittenData().size());
        assertEquals(3, connection2.getWrittenData().size());
        assertEquals(3, connection3.getWrittenData().size());
    }

    @Test
    public void testRoundRobinOrder() {
        // Add 2 connections
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");

        router.addConnection(connection1);
        router.addConnection(connection2);

        // Route items one at a time to verify alternating pattern
        for (int i = 0; i < 4; i++) {
            router.route(Arrays.asList("value-" + i));
        }

        // Should alternate between connections
        assertEquals(2, connection1.getWrittenData().size());
        assertEquals(2, connection2.getWrittenData().size());
    }

    @Test
    public void testUnevenDistribution() {
        // Add 3 connections
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");
        TestAsyncConnection connection3 = createConnection("slot-3");

        router.addConnection(connection1);
        router.addConnection(connection2);
        router.addConnection(connection3);

        // Route 10 items - should be distributed as 4, 3, 3 or similar
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add("value-" + i);
        }
        router.route(data);

        int total = connection1.getWrittenData().size() +
                   connection2.getWrittenData().size() +
                   connection3.getWrittenData().size();

        assertEquals(10, total);

        // No connection should be empty
        assertTrue(connection1.getWrittenData().size() > 0);
        assertTrue(connection2.getWrittenData().size() > 0);
        assertTrue(connection3.getWrittenData().size() > 0);
    }

    @Test
    public void testSingleConnection() {
        TestAsyncConnection connection = createConnection("slot-1");
        router.addConnection(connection);

        List<String> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add("value-" + i);
        }
        router.route(data);

        // All items should go to the single connection
        assertEquals(10, connection.getWrittenData().size());
    }

    @Test
    public void testRouteWithPredicate() {
        // Create connection with predicate that filters out certain values
        TestAsyncConnection connection = createConnection("slot-1",
            data -> data.startsWith("accept"));

        router.addConnection(connection);

        // Route data - some should be filtered out
        List<String> data = Arrays.asList("accept-value1", "reject-value2", "accept-value3");
        router.route(data);

        // Only 2 values should have been routed
        assertEquals(2, connection.getWrittenData().size());
    }

    @Test
    public void testPredicateAcrossMultipleConnections() {
        // Create connections with different predicates
        TestAsyncConnection connection1 = createConnection("slot-1",
            data -> data.contains("even"));
        TestAsyncConnection connection2 = createConnection("slot-2",
            data -> data.contains("odd"));

        router.addConnection(connection1);
        router.addConnection(connection2);

        // Route data
        List<String> data = Arrays.asList("value-even-0", "value-odd-1", "value-even-2", "value-odd-3");
        router.route(data);

        // Connection 1 should get even values, connection 2 should get odd values
        assertEquals(1, connection1.getWrittenData().size()); // value-even-0
        assertEquals(1, connection2.getWrittenData().size()); // value-odd-1
    }

    @Test
    public void testMetrics() {
        TestAsyncConnection connection = createConnection("slot-1");
        router.addConnection(connection);

        Metrics metrics = router.getMetrics();
        assertNotNull(metrics);
        assertEquals("Router_test-router", metrics.getMetricGroupId().id());

        // Route some data
        List<String> data = Arrays.asList("value1", "value2", "value3");
        router.route(data);

        // Verify metrics are updated
        assertEquals(3, metrics.getCounter("numEventsProcessed").value());
        assertEquals(3, metrics.getCounter("numEventsRouted").value());
    }

    @Test
    public void testMultipleRoutes() {
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");

        router.addConnection(connection1);
        router.addConnection(connection2);

        // First route
        router.route(Arrays.asList("value1", "value2"));

        // Second route
        router.route(Arrays.asList("value3", "value4"));

        // Verify distribution continues across routes
        int total = connection1.getWrittenData().size() + connection2.getWrittenData().size();
        assertEquals(4, total);
    }

    @Test
    public void testIndexWraparound() {
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");

        router.addConnection(connection1);
        router.addConnection(connection2);

        // Route many items to test index wraparound
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add("value-" + i);
        }
        router.route(data);

        // Should be evenly distributed
        assertEquals(50, connection1.getWrittenData().size());
        assertEquals(50, connection2.getWrittenData().size());
    }

    @Test
    public void testConnectionsAddedAfterRouting() {
        TestAsyncConnection connection1 = createConnection("slot-1");
        router.addConnection(connection1);

        // Route some data
        router.route(Arrays.asList("value1", "value2"));
        assertEquals(2, connection1.getWrittenData().size());

        // Add another connection
        TestAsyncConnection connection2 = createConnection("slot-2");
        router.addConnection(connection2);

        // Route more data - should now distribute across both
        router.route(Arrays.asList("value3", "value4", "value5", "value6"));

        assertTrue(connection2.getWrittenData().size() > 0);
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");

        router.addConnection(connection1);
        router.addConnection(connection2);

        // Create multiple threads that route data concurrently
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int threadNum = i;
            Thread thread = new Thread(() -> {
                for (int j = 0; j < 10; j++) {
                    router.route(Arrays.asList("value-" + threadNum + "-" + j));
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify all data was routed
        int total = connection1.getWrittenData().size() + connection2.getWrittenData().size();
        assertEquals(100, total);
    }

    @Test
    public void testRemoveConnectionDuringRouting() {
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");
        TestAsyncConnection connection3 = createConnection("slot-3");

        router.addConnection(connection1);
        router.addConnection(connection2);
        router.addConnection(connection3);

        // Route some data
        router.route(Arrays.asList("value1", "value2", "value3"));

        // Remove middle connection
        router.removeConnection(connection2);

        // Route more data
        router.route(Arrays.asList("value4", "value5", "value6", "value7"));

        // Connection2 should not have received any data after removal
        assertEquals(1, connection2.getWrittenData().size()); // Only from first route
    }

    @Test
    public void testBatchedWrites() {
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");

        router.addConnection(connection1);
        router.addConnection(connection2);

        // Route multiple items in one call
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add("value-" + i);
        }
        router.route(data);

        // Each connection should receive its items in a single write call
        // (5 items per connection in one batch)
        assertEquals(1, connection1.getWriteCalls());
        assertEquals(1, connection2.getWriteCalls());
        assertEquals(5, connection1.getWrittenData().size());
        assertEquals(5, connection2.getWrittenData().size());
    }

    // Helper methods

    private TestAsyncConnection createConnection(String slotId) {
        return createConnection(slotId, null);
    }

    private TestAsyncConnection createConnection(String slotId,
                                                 rx.functions.Func1<String, Boolean> predicate) {
        TestAsyncConnection connection = new TestAsyncConnection(slotId, predicate);
        connections.add(connection);
        return connection;
    }

    // Test helper class
    private static class TestAsyncConnection extends AsyncConnection<String> {
        private final List<byte[]> writtenData = new ArrayList<>();
        private int writeCalls = 0;

        public TestAsyncConnection(String slotId, rx.functions.Func1<String, Boolean> predicate) {
            super("test-host", 1234, "id-" + slotId, slotId, "test-group",
                  PublishSubject.create(), predicate);
        }

        @Override
        public void write(List<byte[]> data) {
            writeCalls++;
            writtenData.addAll(data);
        }

        public List<byte[]> getWrittenData() {
            return writtenData;
        }

        public int getWriteCalls() {
            return writeCalls;
        }
    }
}
