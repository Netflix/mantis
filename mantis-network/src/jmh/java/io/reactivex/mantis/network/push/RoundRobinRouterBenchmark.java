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

package io.reactivex.mantis.network.push;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import rx.Observer;
import rx.functions.Func1;

/**
 * Compares {@link RoundRobinRouter#route} against {@link LegacyRoundRobinRouter#route}, which is the
 * verbatim pre-change body.
 *
 * <p>The two arms differ in three places: the legacy one copies the connection {@link Set} into an
 * {@link ArrayList} and {@link java.util.Collections#shuffle(List) shuffle}s it, buffers per-destination
 * writes in a {@link java.util.LinkedList}, and allocates an unsized {@link java.util.HashMap}. The new
 * one advances a looping iterator to a single {@code ThreadLocalRandom} offset and sizes both
 * collections.
 *
 * <p><b>Run multi-threaded.</b> The largest claimed cost is not the per-connection work, it is that
 * {@code Collections.shuffle(List)} draws from a single {@code private static Random} shared by the
 * whole JVM, so every draw is a contended CAS on one seed. That only shows up with more than one
 * router thread, and JMH's thread count is not expressible as a {@code @Param}, so drive it from the
 * CLI:
 *
 * <pre>
 *   ./gradlew :mantis-network:jmhJar
 *   java -jar mantis-network/build/libs/mantis-network-*-jmh.jar RoundRobinRouterBenchmark -t 1
 *   java -jar mantis-network/build/libs/mantis-network-*-jmh.jar RoundRobinRouterBenchmark -t 8
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class RoundRobinRouterBenchmark {

    private static final AtomicInteger NAMES = new AtomicInteger();

    /** Subscribers on one group. Real groups run from a handful to hundreds of thousands. */
    @Param({"2", "8", "64", "512"})
    public int connections;

    /** Events in one drain -- what the 200ms chunker hands to route() at a time. */
    @Param({"30", "200"})
    public int chunkSize;

    private Router<byte[]> current;
    private Router<byte[]> legacy;
    private Set<AsyncConnection<byte[]>> connectionSet;
    private List<byte[]> chunks;

    /**
     * Counts deliveries into a plain field on a per-connection object. Not volatile and not
     * synchronized on purpose: each connection has its own sink, so there is nothing to contend on,
     * and the store still cannot be optimised away because the sink stays reachable from the
     * benchmark state. Deliberately not a Counter -- {@code CounterImpl.value()} reads through to a
     * Spectator registry that is a no-op outside a running worker, so it always reads zero here.
     */
    static final class Sink implements Observer<List<byte[]>> {
        long batches;
        long events;

        @Override
        public void onNext(List<byte[]> data) {
            batches++;
            events += data.size();
        }

        @Override
        public void onCompleted() {
        }

        @Override
        public void onError(Throwable e) {
        }
    }

    private List<Sink> sinks;

    @Setup(Level.Trial)
    public void setup() {
        String tag = "bench" + NAMES.incrementAndGet();
        Func1<byte[], byte[]> encoder = b -> b;
        current = new RoundRobinRouter<>(tag, encoder);
        legacy = new LegacyRoundRobinRouter<>(tag, encoder);

        sinks = new ArrayList<>(connections);
        connectionSet = new HashSet<>(connections * 2);
        for (int i = 0; i < connections; i++) {
            Sink sink = new Sink();
            sinks.add(sink);
            connectionSet.add(new AsyncConnection<>(
                    "host" + i, 7000 + i, "id" + i, "slot" + i, "group", sink, null));
        }

        chunks = new ArrayList<>(chunkSize);
        for (int i = 0; i < chunkSize; i++) {
            byte[] event = new byte[256];
            event[0] = (byte) i;
            chunks.add(event);
        }

        // Cross-check the arms deliver the same volume before measuring anything. Content and
        // destination differ run to run -- both arms randomise the starting connection -- so the
        // invariant that holds is every chunk goes out exactly once.
        long before = totalEvents();
        current.route(connectionSet, chunks);
        long afterCurrent = totalEvents() - before;
        legacy.route(connectionSet, chunks);
        long afterLegacy = totalEvents() - before - afterCurrent;
        if (afterCurrent != chunkSize || afterLegacy != chunkSize) {
            throw new IllegalStateException("arms disagree: current=" + afterCurrent
                    + " legacy=" + afterLegacy + " expected=" + chunkSize);
        }
    }

    private long totalEvents() {
        long total = 0;
        for (Sink sink : sinks) {
            total += sink.events;
        }
        return total;
    }

    @Benchmark
    public void route() {
        current.route(connectionSet, chunks);
    }

    @Benchmark
    public void routeLegacyVerbatim() {
        legacy.route(connectionSet, chunks);
    }
}
