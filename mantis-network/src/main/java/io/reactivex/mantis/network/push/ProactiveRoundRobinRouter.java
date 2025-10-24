package io.reactivex.mantis.network.push;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import rx.functions.Func1;

import java.util.*;

public class ProactiveRoundRobinRouter<T> implements ProactiveRouter<T> {
    private final List<AsyncConnection<T>> connections = new ArrayList<>();
    private int currentIndex = 0;

    protected Func1<T, byte[]> encoder;
    protected final Counter numEventsRouted;
    protected final Counter numEventsProcessed;
    protected final Counter numConnectionUpdates;
    protected final Metrics metrics;

    public ProactiveRoundRobinRouter(String name, Func1<T, byte[]> encoder) {
        this.encoder = encoder;
        metrics = new Metrics.Builder()
            .name("Router_" + name)
            .addCounter("numEventsRouted")
            .addCounter("numEventsProcessed")
            .addCounter("numConnectionUpdates")
            .build();
        numEventsRouted = metrics.getCounter("numEventsRouted");
        numEventsProcessed = metrics.getCounter("numEventsProcessed");
        numConnectionUpdates = metrics.getCounter("numConnectionUpdates");
    }

    @Override
    public synchronized void addConnection(AsyncConnection<T> connection) {
        // We do not need to shuffle because we are constantly looping through
        connections.add(connection);
    }

    @Override
    public synchronized void removeConnection(AsyncConnection<T> connection) {
        connections.remove(connection);
    }

    @Override
    public synchronized void route(List<T> chunks) {
        if (connections.isEmpty() || chunks == null || chunks.isEmpty()) {
            return;
        }
        numEventsProcessed.increment(chunks.size());
        Map<AsyncConnection<T>, List<byte[]>> writes = new HashMap<>();
        int arrayListSize = chunks.size() / connections.size() + 1; // assume even distribution
        // process chunks
        for (T chunk : chunks) {
            currentIndex = currentIndex % connections.size();
            AsyncConnection<T> connection = connections.get(currentIndex);
            Func1<T, Boolean> predicate = connection.getPredicate();
            if (predicate == null || predicate.call(chunk)) {
                List<byte[]> buffer = writes.computeIfAbsent(connection, k -> new ArrayList<>(arrayListSize));
                buffer.add(encoder.call(chunk));
                currentIndex++;
            }
        }
        for (Map.Entry<AsyncConnection<T>, List<byte[]>> entry : writes.entrySet()) {
            AsyncConnection<T> connection = entry.getKey();
            List<byte[]> toWrite = entry.getValue();
            connection.write(toWrite);
            numEventsRouted.increment(toWrite.size());
        }
    }

    @Override
    public Metrics getMetrics() {
        return metrics;
    }
}
