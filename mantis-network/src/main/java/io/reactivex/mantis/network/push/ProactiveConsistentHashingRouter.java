package io.reactivex.mantis.network.push;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ProactiveConsistentHashingRouter<K, V> implements ProactiveRouter<KeyValuePair<K, V>> {
    private static final Logger logger = LoggerFactory.getLogger(ProactiveConsistentHashingRouter.class);
    private static final int connectionRepetitionOnRing = 1000;

    protected final Func1<KeyValuePair<K, V>, byte[]> encoder;
    protected final Counter numEventsRouted;
    protected final Counter numEventsProcessed;
    protected final Counter numConnectionUpdates;
    protected final Metrics metrics;
    private final HashFunction hashFunction;
    private final NavigableMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring = new TreeMap<>();
    private final ReadWriteLock ringLock = new ReentrantReadWriteLock();

    public ProactiveConsistentHashingRouter(
        String name,
        Func1<KeyValuePair<K, V>, byte[]> dataEncoder,
        HashFunction hashFunction) {
        this.encoder = dataEncoder;
        metrics = new Metrics.Builder()
            .name("Router_" + name)
            .addCounter("numEventsRouted")
            .addCounter("numEventsProcessed")
            .addCounter("numConnectionUpdates")
            .build();
        numEventsRouted = metrics.getCounter("numEventsRouted");
        numEventsProcessed = metrics.getCounter("numEventsProcessed");
        numConnectionUpdates = metrics.getCounter("numConnectionUpdates");
        this.hashFunction = hashFunction;
    }

    @Override
    public void route(List<KeyValuePair<K, V>> chunks) {
        if (chunks == null || chunks.isEmpty()) {
            return;
        }

        // Read lock only for ring access
        Map<AsyncConnection<KeyValuePair<K, V>>, List<byte[]>> writes;
        ringLock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return;
            }

            int numConnections = ring.size() / connectionRepetitionOnRing;
            int bufferCapacity = (chunks.size() / numConnections) + 1; // assume even distribution
            writes = new HashMap<>(numConnections);

            // process chunks (ring access inside lookupConnection)
            for (KeyValuePair<K, V> kvp : chunks) {
                long hash = kvp.getKeyBytesHashed();
                // lookup slot
                AsyncConnection<KeyValuePair<K, V>> connection = lookupConnection(hash);
                // add to writes
                Func1<KeyValuePair<K, V>, Boolean> predicate = connection.getPredicate();
                if (predicate == null || predicate.call(kvp)) {
                    List<byte[]> buffer = writes.computeIfAbsent(connection, k -> new ArrayList<>(bufferCapacity));
                    buffer.add(encoder.call(kvp));
                }
            }
        } finally {
            ringLock.readLock().unlock();
        }

        // process writes (outside lock - no ring access)
        if (!writes.isEmpty()) {
            for (Map.Entry<AsyncConnection<KeyValuePair<K, V>>, List<byte[]>> entry : writes.entrySet()) {
                AsyncConnection<KeyValuePair<K, V>> connection = entry.getKey();
                List<byte[]> toWrite = entry.getValue();
                connection.write(toWrite);
                numEventsRouted.increment(toWrite.size());
            }
        }
    }

    @Override
    public void addConnection(AsyncConnection<KeyValuePair<K, V>> connection) {
        String connectionId = connection.getSlotId();
        if (connectionId == null) {
            throw new IllegalStateException("Connection must specify an id for consistent hashing");
        }

        List<String> hashCollisions = new ArrayList<>();
        ringLock.writeLock().lock();
        try {
            for (int i = 0; i < connectionRepetitionOnRing; i++) {
                // hash node on ring
                byte[] connectionBytes = (connectionId + "-" + i).getBytes();
                long hash = hashFunction.computeHash(connectionBytes);
                if (ring.containsKey(hash)) {
                    hashCollisions.add(connectionId + "-" + i);
                }
                ring.put(hash, connection);
            }
        } finally {
            ringLock.writeLock().unlock();
        }

        // Log outside lock
        if (!hashCollisions.isEmpty()) {
            logger.error("Hash collisions detected when adding connection {}: {}", connectionId, hashCollisions);
        }
    }

    @Override
    public void removeConnection(AsyncConnection<KeyValuePair<K, V>> connection) {
        String connectionId = connection.getSlotId();
        if (connectionId == null) {
            throw new IllegalStateException("Connection must specify an id for consistent hashing");
        }

        ringLock.writeLock().lock();
        try {
            for (int i = 0; i < connectionRepetitionOnRing; i++) {
                // hash node on ring
                byte[] connectionBytes = (connectionId + "-" + i).getBytes();
                long hash = hashFunction.computeHash(connectionBytes);
                ring.remove(hash);
            }
        } finally {
            ringLock.writeLock().unlock();
        }
    }

    @Override
    public Metrics getMetrics() {
        return metrics;
    }

    private AsyncConnection<KeyValuePair<K, V>> lookupConnection(long hash) {
        Map.Entry<Long, AsyncConnection<KeyValuePair<K, V>>> connection = ring.ceilingEntry(hash);
        return (connection == null ? ring.firstEntry() : connection).getValue();
    }
}
