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

package io.mantisrx.common.network;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;


/**
 * A Consistent Hashing implementation class which can work with existing node
 * types representing nodes to which requests are handed.
 * <p>
 * This class is by design not thread-safe for node mutations
 * (Additions/removals) Such mutations should be handled externally and a new
 * instance of this class should be created with the new node list. This allows
 * for lock free implementation in this class keeping the code performant. (For
 * instance, in the caller application there can be a thread to poll discovery
 * every so often to get an updated node list and if there are differences
 * between the previously held list and the new one, a new instance of
 * {@link ConsistentHash} can be created and the old one can be swapped with the
 * new one. Also the calling application can decide how nodes map to keys used
 * for hashing through the {@link NodeConfiguration} implementation - for
 * instance they can ensure that new nodes take the same "slot" as an old one it
 * is replacing by keeping track of global slots.)
 * <p>
 * Callers should verify that the node returned by
 * {@link ConsistentHash#get(byte[])} is alive before using it to retrieve the
 * value.
 * <p>
 * Copied mostly from http://weblogs.java.net/blog/2007/11/27/consistent-hashing
 * Also inspired by KetamaNodeLocator in spy memcached code
 *
 * @param <T> Type representing a node object
 *
 * @author pkamath
 */
public class ConsistentHash<T> {

    private final HashFunction hashAlgo;
    private final NodeConfiguration<T> nodeConfig;
    private final SortedMap<Long, T> ring = new TreeMap<Long, T>();


    public ConsistentHash(HashFunction hashAlgo,
                          NodeConfiguration<T> nodeConfig, Collection<T> nodes) {
        if (hashAlgo == null || nodeConfig == null || nodes == null
                || nodes.isEmpty()) {
            throw new IllegalArgumentException(
                    "Constructor args to "
                            + "ConsistentHash should be non null and the collection of "
                            + "nodes should be non empty");
        }
        this.hashAlgo = hashAlgo;
        this.nodeConfig = nodeConfig;

        for (T node : nodes) {
            add(node);
        }
    }

    private void add(T node) {
        int numReps = nodeConfig.getNodeRepetitions();
        if (numReps < 0) {
            throw new IllegalArgumentException("Number of repetitions of a "
                    + "node should be positive");
        }
        for (int i = 0; i < numReps; i++) {
            putInRing(hashAlgo.call(nodeConfig.getKeyForNode(node, i)),
                    node);
        }
    }

    void putInRing(Long hash, T node) {
        if (ring.containsKey(hash)) {
            //            LOG.warn("Duplicate virtual node being added to ring - possibly due to a faulty implementation "
            //                    + "of NodeConfiguration.getKeyForNode() which is not returning unique values for different "
            //                    + "repititions");
        }
        ring.put(hash, node);
    }

    /**
     * Returns the node which should contain the supplied key per consistent
     * hashing algorithm
     *
     * @param keyBytes key to search on
     *
     * @return Node which should contain the key, null if no Nodes are present
     * Callers should verify that the node returned is alive before
     * using it to retrieve the value.
     */
    public T get(byte[] keyBytes) {
        Long hash = hashAlgo.call(keyBytes);
        if (!ring.containsKey(hash)) {
            SortedMap<Long, T> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    // for unit tests - package access only
    SortedMap<Long, T> getRing() {
        return ring;
    }

    HashFunction getHashAlgo() {
        return hashAlgo;
    }
}
