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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;


public class ConsistentHashingRouter<K, V> extends Router<KeyValuePair<K, V>> {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashingRouter.class);
    private static int connectionRepetitionOnRing = 1000;
    private static long validCacheAgeMSec = 5000;
    private HashFunction hashFunction;
    private AtomicReference<SnapshotCache<SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>>>> cachedRingRef = new AtomicReference<>();

    public ConsistentHashingRouter(String name,
                                   Func1<KeyValuePair<K, V>, byte[]> dataEncoder,
                                   HashFunction hashFunction) {
        super("ConsistentHashingRouter_" + name, dataEncoder);
        this.hashFunction = hashFunction;
    }

    @Override
    public void route(Set<AsyncConnection<KeyValuePair<K, V>>> connections,
                      List<KeyValuePair<K, V>> chunks) {
        if (connections != null && !connections.isEmpty() &&
                chunks != null && !chunks.isEmpty()) {

            int numConnections = connections.size();
            int bufferCapacity = (chunks.size() / numConnections) + 1; // assume even distribution
            Map<AsyncConnection<KeyValuePair<K, V>>, List<byte[]>> writes = new HashMap<>(numConnections);

            // hash connections into slots
            SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring =
                    hashConnections(connections);

            // process chunks
            for (KeyValuePair<K, V> kvp : chunks) {
                long hash = kvp.getKeyBytesHashed();
                // lookup slot
                AsyncConnection<KeyValuePair<K, V>> connection = lookupConnection(hash, ring);
                // add to writes
                Func1<KeyValuePair<K, V>, Boolean> predicate = connection.getPredicate();
                if (predicate == null || predicate.call(kvp)) {
                    List<byte[]> buffer = writes.get(connection);
                    if (buffer == null) {
                        buffer = new ArrayList<>(bufferCapacity);
                        writes.put(connection, buffer);
                    }
                    buffer.add(encoder.call(kvp));
                }
            }

            // process writes
            if (!writes.isEmpty()) {
                for (Entry<AsyncConnection<KeyValuePair<K, V>>, List<byte[]>> entry : writes.entrySet()) {
                    AsyncConnection<KeyValuePair<K, V>> connection = entry.getKey();
                    List<byte[]> toWrite = entry.getValue();
                    connection.write(toWrite);
                    numEventsRouted.increment(toWrite.size());
                }
            }
        }
    }

    private AsyncConnection<KeyValuePair<K, V>> lookupConnection(long hash, SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring) {
        if (!ring.containsKey(hash)) {
            SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    private void computeRing(Set<AsyncConnection<KeyValuePair<K, V>>> connections) {
        SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring = new TreeMap<Long, AsyncConnection<KeyValuePair<K, V>>>();
        for (AsyncConnection<KeyValuePair<K, V>> connection : connections) {
            for (int i = 0; i < connectionRepetitionOnRing; i++) {
                // hash node on ring
                String connectionId = connection.getSlotId();
                if (connectionId == null) {
                    throw new IllegalStateException("Connection must specify an id for consistent hashing");
                }
                byte[] connectionBytes = (connectionId + "-" + i).getBytes();
                long hash = hashFunction.computeHash(connectionBytes);
                if (ring.containsKey(hash)) {
                    logger.error("Hash collision when computing ring. {} hashed to a value already in the ring.", connectionId + "-" + i);
                }
                ring.put(hash, connection);
            }
        }
        cachedRingRef.set(new SnapshotCache<SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>>>(ring));
    }

    private SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> hashConnections(Set<AsyncConnection<KeyValuePair<K, V>>> connections) {

        SnapshotCache<SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>>> cache = cachedRingRef.get();

        if (cache == null) {
            logger.info("Recomputing ring due to null reference");
            computeRing(connections);
        } else {
            SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> cachedRing = cache.getCache();
            // determine if need to recompute cache
            if (cachedRing.size() != (connections.size() * connectionRepetitionOnRing)) {
                // number of connections not equal
                logger.info("Recomputing ring due to difference in number of connections ({}) versus cache size ({}).", connections.size() * connectionRepetitionOnRing, cachedRing.size());
                computeRing(connections);
            } else {
                // number of connections equal, check timestamp
                long timestamp = cache.getTimestamp();
                if (System.currentTimeMillis() - timestamp > validCacheAgeMSec) {
                    computeRing(connections);
                }
            }
        }
        return cachedRingRef.get().getCache();
    }
}
