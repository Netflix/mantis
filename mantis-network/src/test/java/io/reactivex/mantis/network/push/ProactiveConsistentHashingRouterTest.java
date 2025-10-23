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

public class ProactiveConsistentHashingRouterTest {

    private ProactiveConsistentHashingRouter<String, String> router;
    private HashFunction hashFunction;
    private List<TestAsyncConnection> connections;

    @BeforeEach
    public void setup() {
        hashFunction = HashFunctions.xxh3();
        router = new ProactiveConsistentHashingRouter<>("test-router",
            kvp -> kvp.getValue().getBytes(), hashFunction);
        connections = new ArrayList<>();
    }

    @Test
    public void testAddConnection() {
        TestAsyncConnection connection = createConnection("slot-1");

        router.addConnection(connection);

        // Route some data to verify connection was added
        List<KeyValuePair<String, String>> data = createTestData("key1", "value1");
        router.route(data);

        assertTrue(connection.getWrittenData().size() > 0);
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
        List<KeyValuePair<String, String>> data = createTestData("key1", "value1");
        router.route(data);

        assertEquals(0, connection1.getWrittenData().size());
        assertTrue(connection2.getWrittenData().size() > 0);
    }

    @Test
    public void testRouteWithNoConnections() {
        List<KeyValuePair<String, String>> data = createTestData("key1", "value1");

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
    public void testConsistentHashing() {
        // Add multiple connections
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");
        TestAsyncConnection connection3 = createConnection("slot-3");

        router.addConnection(connection1);
        router.addConnection(connection2);
        router.addConnection(connection3);

        // Route the same key multiple times - should always go to same connection
        String key = "consistent-key";
        for (int i = 0; i < 5; i++) {
            List<KeyValuePair<String, String>> data = createTestData(key, "value" + i);
            router.route(data);
        }

        // Find which connection received the data
        TestAsyncConnection targetConnection = null;
        for (TestAsyncConnection conn : Arrays.asList(connection1, connection2, connection3)) {
            if (conn.getWrittenData().size() > 0) {
                if (targetConnection == null) {
                    targetConnection = conn;
                } else {
                    // Should only be one connection receiving data for this key
                    fail("Data was routed to multiple connections for the same key");
                }
            }
        }

        assertNotNull(targetConnection);
        assertEquals(5, targetConnection.getWrittenData().size());
    }

    @Test
    public void testDistributionAcrossMultipleConnections() {
        // Add multiple connections
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");
        TestAsyncConnection connection3 = createConnection("slot-3");

        router.addConnection(connection1);
        router.addConnection(connection2);
        router.addConnection(connection3);

        // Route many different keys
        List<KeyValuePair<String, String>> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            long hash = hashFunction.computeHash(key.getBytes());
            data.add(new KeyValuePair<>(hash, key.getBytes(), "value-" + i));
        }
        router.route(data);

        // Verify all connections received some data (probabilistically should happen with 100 keys)
        int totalRouted = connection1.getWrittenData().size() +
                         connection2.getWrittenData().size() +
                         connection3.getWrittenData().size();

        assertEquals(100, totalRouted);
        assertTrue(connection1.getWrittenData().size() > 0);
        assertTrue(connection2.getWrittenData().size() > 0);
        assertTrue(connection3.getWrittenData().size() > 0);
    }

    @Test
    public void testRouteWithPredicate() {
        // Create connection with predicate that filters out certain values
        TestAsyncConnection connection = createConnection("slot-1",
            kvp -> kvp.getValue().startsWith("accept"));

        router.addConnection(connection);

        // Route data - some should be filtered out
        List<KeyValuePair<String, String>> data = new ArrayList<>();
        data.add(createKeyValuePair("key1", "accept-value1"));
        data.add(createKeyValuePair("key2", "reject-value2"));
        data.add(createKeyValuePair("key3", "accept-value3"));

        router.route(data);

        // Only 2 values should have been routed
        assertEquals(2, connection.getWrittenData().size());
    }

    @Test
    public void testAddConnectionWithNullSlotId() {
        PublishSubject<List<byte[]>> subject = PublishSubject.create();
        AsyncConnection<KeyValuePair<String, String>> connection =
            new AsyncConnection<>("host", 1234, "id1", null, "group1", subject, null);

        assertThrows(IllegalStateException.class, () -> router.addConnection(connection));
    }

    @Test
    public void testRemoveConnectionWithNullSlotId() {
        PublishSubject<List<byte[]>> subject = PublishSubject.create();
        AsyncConnection<KeyValuePair<String, String>> connection =
            new AsyncConnection<>("host", 1234, "id1", null, "group1", subject, null);

        assertThrows(IllegalStateException.class, () -> router.removeConnection(connection));
    }

    @Test
    public void testMetrics() {
        TestAsyncConnection connection = createConnection("slot-1");
        router.addConnection(connection);

        Metrics metrics = router.getMetrics();
        assertNotNull(metrics);
        assertEquals("Router_test-router", metrics.getMetricGroupId().id());

        // Route some data
        List<KeyValuePair<String, String>> data = createTestData("key1", "value1");
        router.route(data);

        // Verify metrics are updated
        assertTrue(metrics.getCounter("numEventsRouted").value() > 0);
    }

    @Test
    public void testMultipleDataItemsInSingleRoute() {
        TestAsyncConnection connection1 = createConnection("slot-1");
        TestAsyncConnection connection2 = createConnection("slot-2");

        router.addConnection(connection1);
        router.addConnection(connection2);

        // Route multiple items at once
        List<KeyValuePair<String, String>> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(createKeyValuePair("key-" + i, "value-" + i));
        }

        router.route(data);

        int totalRouted = connection1.getWrittenData().size() + connection2.getWrittenData().size();
        assertEquals(10, totalRouted);
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        TestAsyncConnection connection = createConnection("slot-1");
        router.addConnection(connection);

        // Create multiple threads that route data concurrently
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int threadNum = i;
            Thread thread = new Thread(() -> {
                for (int j = 0; j < 10; j++) {
                    List<KeyValuePair<String, String>> data =
                        createTestData("key-" + threadNum + "-" + j, "value-" + threadNum + "-" + j);
                    router.route(data);
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
        assertEquals(100, connection.getWrittenData().size());
    }

    // Helper methods

    private TestAsyncConnection createConnection(String slotId) {
        return createConnection(slotId, null);
    }

    private TestAsyncConnection createConnection(String slotId,
                                                 rx.functions.Func1<KeyValuePair<String, String>, Boolean> predicate) {
        TestAsyncConnection connection = new TestAsyncConnection(slotId, predicate);
        connections.add(connection);
        return connection;
    }

    private List<KeyValuePair<String, String>> createTestData(String key, String value) {
        List<KeyValuePair<String, String>> data = new ArrayList<>();
        data.add(createKeyValuePair(key, value));
        return data;
    }

    private KeyValuePair<String, String> createKeyValuePair(String key, String value) {
        long hash = hashFunction.computeHash(key.getBytes());
        return new KeyValuePair<>(hash, key.getBytes(), value);
    }

    // Test helper class
    private static class TestAsyncConnection extends AsyncConnection<KeyValuePair<String, String>> {
        private final List<byte[]> writtenData = new ArrayList<>();
        private int writeCalls = 0;

        public TestAsyncConnection(String slotId,
                                  rx.functions.Func1<KeyValuePair<String, String>, Boolean> predicate) {
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
