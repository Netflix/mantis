/*
 * Copyright 2026 Netflix, Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import rx.Observer;
import rx.functions.Func1;

/**
 * Behavioural pins for {@link RoundRobinRouter#route}. The change under test replaced a full
 * {@code Collections.shuffle} of the connection set with a single random start offset, and swapped
 * the per-destination {@code LinkedList} for a pre-sized {@code ArrayList}; neither is observable
 * from the outside, so what these tests assert is the routing contract that must not move.
 */
class RoundRobinRouterTest {

    /** Records what a connection was handed, in order. */
    private static final class Recorder implements Observer<List<byte[]>> {

        private final List<List<String>> writes = new ArrayList<>();

        @Override
        public void onNext(List<byte[]> data) {
            writes.add(data.stream()
                    .map(b -> new String(b, StandardCharsets.UTF_8))
                    .collect(Collectors.toList()));
        }

        @Override
        public void onError(Throwable e) {
            throw new AssertionError(e);
        }

        @Override
        public void onCompleted() {
        }

        List<String> received() {
            return writes.stream().flatMap(List::stream).collect(Collectors.toList());
        }
    }

    private static RoundRobinRouter<String> router(String name) {
        return new RoundRobinRouter<>(name, s -> s.getBytes(StandardCharsets.UTF_8));
    }

    /** Connections keyed by id, insertion-ordered so failures are readable. */
    private static Map<String, Recorder> connect(
            Set<AsyncConnection<String>> into, int count, Func1<String, Boolean> predicate) {
        Map<String, Recorder> recorders = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String id = "conn-" + i;
            Recorder recorder = new Recorder();
            recorders.put(id, recorder);
            into.add(new AsyncConnection<>("host", 1000 + i, id, id, "group", recorder, predicate));
        }
        return recorders;
    }

    private static List<String> chunks(int count) {
        return IntStream.range(0, count).mapToObj(i -> "event-" + i).collect(Collectors.toList());
    }

    @Test
    void everyChunkIsDeliveredExactlyOnceAndSpreadEvenly() {
        Set<AsyncConnection<String>> connections = new LinkedHashSet<>();
        Map<String, Recorder> recorders = connect(connections, 4, null);
        List<String> chunks = chunks(10);

        RoundRobinRouter<String> router = router("even-spread");
        router.route(connections, chunks);

        List<String> delivered = recorders.values().stream()
                .flatMap(r -> r.received().stream())
                .collect(Collectors.toList());
        assertEquals(chunks.size(), delivered.size(), "no chunk may be dropped or duplicated");
        assertEquals(new HashSet<>(chunks), new HashSet<>(delivered));

        // 10 chunks over 4 connections is 3 or 2 each, whatever the start offset.
        for (Map.Entry<String, Recorder> e : recorders.entrySet()) {
            int size = e.getValue().received().size();
            assertTrue(size == 2 || size == 3, e.getKey() + " received " + size);
        }
    }

    /**
     * Each destination gets one write holding all of its chunks -- not one write per chunk. This is
     * what makes the downstream batching worthwhile, so it is worth pinning.
     */
    @Test
    void eachDestinationReceivesASingleBatchedWrite() {
        Set<AsyncConnection<String>> connections = new LinkedHashSet<>();
        Map<String, Recorder> recorders = connect(connections, 2, null);

        router("batched").route(connections, chunks(6));

        for (Map.Entry<String, Recorder> e : recorders.entrySet()) {
            assertEquals(1, e.getValue().writes.size(), e.getKey() + " should get one write");
            assertEquals(3, e.getValue().received().size());
        }
    }

    /** Fewer chunks than connections: only the chunks.size() connections in line get a write. */
    @Test
    void connectionsWithNoChunksAreNotWrittenTo() {
        Set<AsyncConnection<String>> connections = new LinkedHashSet<>();
        Map<String, Recorder> recorders = connect(connections, 10, null);

        router("sparse").route(connections, chunks(3));

        long written = recorders.values().stream().filter(r -> !r.writes.isEmpty()).count();
        assertEquals(3, written, "exactly one write per chunk, to distinct connections");
    }

    /**
     * A chunk assigned to a connection whose predicate rejects it is dropped, not handed on to the
     * next connection. That was the pre-existing behaviour and this change does not alter it.
     */
    @Test
    void chunksRejectedByAPredicateAreDroppedNotReRouted() {
        Set<AsyncConnection<String>> connections = new LinkedHashSet<>();
        Map<String, Recorder> recorders = connect(connections, 3, s -> s.endsWith("0"));
        List<String> chunks = chunks(9);

        RoundRobinRouter<String> router = router("predicate");
        router.route(connections, chunks);

        List<String> delivered = recorders.values().stream()
                .flatMap(r -> r.received().stream())
                .collect(Collectors.toList());
        assertEquals(List.of("event-0"), delivered);
    }

    /**
     * The start offset must keep moving: with a single chunk and many connections, load only spreads
     * if successive calls begin somewhere else. Over 2000 calls across 50 connections the chance of
     * any connection being missed by a uniform draw is about 1e-16, so this is not flaky.
     */
    @Test
    void startOffsetIsRandomisedAcrossCallsSoASingleChunkSpreads() {
        Set<AsyncConnection<String>> connections = new LinkedHashSet<>();
        Map<String, Recorder> recorders = connect(connections, 50, null);

        RoundRobinRouter<String> router = router("spread");
        for (int i = 0; i < 2000; i++) {
            router.route(connections, List.of("event-" + i));
        }

        long touched = recorders.values().stream().filter(r -> !r.writes.isEmpty()).count();
        assertEquals(50, touched, "every connection should have been the start at least once");
    }

    @Test
    void emptyAndNullChunkBatchesAreNoOps() {
        Set<AsyncConnection<String>> connections = new LinkedHashSet<>();
        Map<String, Recorder> recorders = connect(connections, 3, null);

        RoundRobinRouter<String> router = router("empty");
        router.route(connections, List.of());
        router.route(connections, null);

        assertTrue(recorders.values().stream().allMatch(r -> r.writes.isEmpty()));
    }

    @Test
    void noConnectionsDropsTheBatchWithoutThrowing() {
        Set<AsyncConnection<String>> connections = new LinkedHashSet<>();
        Map<String, Recorder> recorders = connect(connections, 2, null);
        // deliberately route to an empty set, not to `connections`
        router("no-connections").route(new HashSet<>(), chunks(5));
        assertTrue(recorders.values().stream().allMatch(r -> r.writes.isEmpty()));
    }
}
