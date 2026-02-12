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
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ProactiveRoundRobinRouterTest {

    private ProactiveRoundRobinRouter<String> router;

    @BeforeEach
    public void setup() {
        router = new ProactiveRoundRobinRouter<>("test-router", String::getBytes);
    }

    @Test
    public void testRouteWithNoConnections() {
        List<String> data = Arrays.asList("test-value");

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
    public void testPredicateAcrossMultipleConnections() {
        // Create connections with different predicates
        Func1<String, Boolean> predicate = data -> data.contains("even");
        TestAsyncConnection connection1 = createConnection("slot-1", predicate);
        TestAsyncConnection connection2 = createConnection("slot-2", predicate);

        router.addConnection(connection1);
        router.addConnection(connection2);

        List<String> data = Arrays.asList("value-even-0", "value-odd-1", "value-even-2", "value-odd-3");
        router.route(data);

        assertEquals(1, connection1.getWrittenData().size());
        assertEquals(1, connection2.getWrittenData().size());
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

        assertEquals(2, connection2.getWrittenData().size());
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
        assertEquals(3, connection1.getWrittenData().size());
        assertEquals(1, connection2.getWrittenData().size()); // Only from first route
        assertEquals(3, connection3.getWrittenData().size());
    }

    // Helper methods

    private TestAsyncConnection createConnection(String slotId) {
        return createConnection(slotId, null);
    }

    private TestAsyncConnection createConnection(String slotId,
                                                 rx.functions.Func1<String, Boolean> predicate) {
        return new TestAsyncConnection(slotId, predicate);
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
