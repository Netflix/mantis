/*
 * Copyright 2025 Netflix, Inc.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rx.subjects.PublishSubject;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProactiveConsistentHashingRouterTest {

    private ProactiveConsistentHashingRouter<String, String> router;
    private HashFunction hashFunction;

    @BeforeEach
    public void setup() {
        hashFunction = HashFunctions.xxh3();
        router = new ProactiveConsistentHashingRouter<>("test-router",
            kvp -> kvp.getValue().getBytes(), hashFunction);
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
        assertEquals(5, connection1.getWrittenData().size());
        assertEquals(0, connection2.getWrittenData().size());
        assertEquals(0, connection3.getWrittenData().size());
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
        int routed = 30000;
        for (int i = 0; i < routed; i++) {
            String key = "key-" + i;
            long hash = hashFunction.computeHash(key.getBytes());
            data.add(new KeyValuePair<>(hash, key.getBytes(), "value-" + i));
        }
        router.route(data);

        int actualRouted = connection1.getWrittenData().size() +
                         connection2.getWrittenData().size() +
                         connection3.getWrittenData().size();

        assertEquals(routed, actualRouted);
        // roughly even distribution, but allow 2% variance
        assertEquals(10000, connection1.getWrittenData().size(), routed / 50.0);
        assertEquals(10000, connection2.getWrittenData().size(), routed / 50.0);
        assertEquals(10000, connection3.getWrittenData().size(), routed / 50.0);
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

    // Helper methods

    private TestAsyncConnection createConnection(String slotId) {
        return createConnection(slotId, null);
    }

    private TestAsyncConnection createConnection(String slotId,
                                                 rx.functions.Func1<KeyValuePair<String, String>, Boolean> predicate) {
        return new TestAsyncConnection(slotId, predicate);
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
        private final List<byte[]> writtenData = Collections.synchronizedList(new ArrayList<>());
        private int writeCalls = 0;

        public TestAsyncConnection(String slotId,
                                  rx.functions.Func1<KeyValuePair<String, String>, Boolean> predicate) {
            super("test-host", 1234, "id-" + slotId, slotId, "test-group",
                  PublishSubject.create(), predicate);
        }

        @Override
        public synchronized void write(List<byte[]> data) {
            writeCalls++;
            writtenData.addAll(data);
        }

        public synchronized List<byte[]> getWrittenData() {
            return new ArrayList<>(writtenData);
        }
    }
}
