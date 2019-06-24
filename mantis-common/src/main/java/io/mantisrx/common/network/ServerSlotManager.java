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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerSlotManager<T> {

    private static final String CLIENT_ID = "clientId";
    private static final Logger LOG = LoggerFactory.getLogger(ServerSlotManager.class);
    ConcurrentHashMap<String, SlotAssignmentManager<T>> slotManagerMap = new ConcurrentHashMap<String, SlotAssignmentManager<T>>();
    private HashFunction hashAlgorithm;

    public ServerSlotManager(HashFunction hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }

    public synchronized SlotAssignmentManager<T> registerServer(WritableEndpoint<T> node, Map<String, List<String>> params) {
        LOG.info("Registering server.node: " + node);
        String subId = getSubscriptionId(params);
        if (subId == null) {
            subId = node.getSlotId();
        }
        SlotAssignmentManager<T> sam = slotManagerMap.get(subId);
        // if slot manager doesn't already exist. create it
        if (sam == null) {
            LOG.info("Setting up new SlotAssignmentManager for sub: " + subId);
            sam = new SlotAssignmentManager<T>(hashAlgorithm, subId);
            slotManagerMap.putIfAbsent(subId, sam);
        }
        sam.registerServer(node);
        return sam;
    }

    public synchronized void deregisterServer(WritableEndpoint<T> node, Map<String, List<String>> params) {

        String subId = getSubscriptionId(params);
        if (subId == null) {
            subId = node.getSlotId();
        }
        SlotAssignmentManager<T> sam = slotManagerMap.get(subId);
        if (sam != null) {
            sam.deregisterServer(node);
            // if its empty remove it
            if (sam.isEmpty()) {

                slotManagerMap.remove(subId);
            }
        }
    }


    private String getSubscriptionId(Map<String, List<String>> queryParams) {

        if (queryParams != null && !queryParams.isEmpty()) {

            List<String> subIdList = queryParams.get(CLIENT_ID);
            if (subIdList != null && !subIdList.isEmpty()) {
                return subIdList.get(0);
            }
        }
        return null;
    }


    public static class SlotAssignmentManager<T> {

        AtomicReference<ConsistentHash<WritableEndpoint<T>>> consistentHashRef = new AtomicReference<ConsistentHash<WritableEndpoint<T>>>();
        ConcurrentSkipListSet<WritableEndpoint<T>> nodeList = new ConcurrentSkipListSet<WritableEndpoint<T>>();
        ConcurrentHashMap<String, Integer> connectionIdToSlotNumberMap = new ConcurrentHashMap<String, Integer>();
        private String consumerJobId;
        private HashFunction hashAlgo;
        private Gauge nodesOnRing;

        public SlotAssignmentManager(HashFunction hashAlgo, String subId) {
            this.consumerJobId = subId;
            this.hashAlgo = hashAlgo;
            Metrics metrics = new Metrics.Builder()
                    .name("SlottingRing_" + consumerJobId)
                    .addGauge("nodeCount")
                    .build();

            metrics = MetricsRegistry.getInstance().registerAndGet(metrics);
            nodesOnRing = metrics.getGauge("nodeCount");
        }

        public synchronized boolean forceRegisterServer(WritableEndpoint<T> sn) {
            LOG.info("Ring: " + consumerJobId + " before force register: " + nodeList);
            boolean success = nodeList.add(sn);
            if (!success) {
                // force add, existing connection exists with slot
                WritableEndpoint<T> oldEndpoint = nodeList.tailSet(sn, true).first();
                boolean removed = nodeList.remove(oldEndpoint);
                if (removed) {
                    success = nodeList.add(sn);
                    LOG.info("Explicitly would have closed endpoint: " + oldEndpoint);
                    //oldEndpoint.explicitClose();
                }
            }
            LOG.info("node " + sn + " add " + success);
            LOG.info("Ring: " + consumerJobId + " after force register: " + nodeList);
            ConsistentHash<WritableEndpoint<T>> newConsistentHash = new ConsistentHash<WritableEndpoint<T>>(hashAlgo, new WritableEndpointConfiguration<T>(), nodeList);
            consistentHashRef.set(newConsistentHash);
            nodesOnRing.set(nodeList.size());
            return success;
        }

        public synchronized boolean registerServer(WritableEndpoint<T> sn) {
            LOG.info("Ring: " + consumerJobId + " before register: " + nodeList);
            boolean success = nodeList.add(sn);
            LOG.info("node " + sn + " add " + success);
            LOG.info("Ring: " + consumerJobId + " after register: " + nodeList);
            ConsistentHash<WritableEndpoint<T>> newConsistentHash = new ConsistentHash<WritableEndpoint<T>>(hashAlgo, new WritableEndpointConfiguration<T>(), nodeList);
            consistentHashRef.set(newConsistentHash);
            nodesOnRing.set(nodeList.size());
            return success;
        }

        public synchronized boolean deregisterServer(WritableEndpoint<T> node) {
            LOG.info("Ring: " + consumerJobId + " before deregister: " + nodeList);
            boolean success = nodeList.remove(node);
            LOG.info("node " + node + " removed " + success);
            LOG.info("Ring: " + consumerJobId + " after deregister: " + nodeList);
            if (!nodeList.isEmpty()) {
                ConsistentHash<WritableEndpoint<T>> newConsistentHash = new ConsistentHash<WritableEndpoint<T>>(hashAlgo, new WritableEndpointConfiguration<T>(), nodeList);
                consistentHashRef.set(newConsistentHash);
            }
            nodesOnRing.set(nodeList.size());
            return success;
        }

        public boolean filter(WritableEndpoint<T> node, byte[] keyBytes) {
            if (nodeList.size() > 1) {
                return node.equals(consistentHashRef.get().get(keyBytes));
            } else {
                return true;
            }

        }

        public Collection<WritableEndpoint<T>> endpoints() {
            return nodeList;
        }

        public WritableEndpoint<T> lookup(byte[] keyBytes) {
            return consistentHashRef.get().get(keyBytes);
        }

        public boolean isEmpty() {
            return nodeList.isEmpty();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime
                    * result
                    + ((connectionIdToSlotNumberMap == null) ? 0
                    : connectionIdToSlotNumberMap.hashCode());
            result = prime * result
                    + ((consumerJobId == null) ? 0 : consumerJobId.hashCode());
            result = prime * result
                    + ((nodeList == null) ? 0 : nodeList.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SlotAssignmentManager other = (SlotAssignmentManager) obj;
            if (connectionIdToSlotNumberMap == null) {
                if (other.connectionIdToSlotNumberMap != null)
                    return false;
            } else if (!connectionIdToSlotNumberMap
                    .equals(other.connectionIdToSlotNumberMap))
                return false;
            if (consumerJobId == null) {
                if (other.consumerJobId != null)
                    return false;
            } else if (!consumerJobId.equals(other.consumerJobId))
                return false;
            if (nodeList == null) {
                if (other.nodeList != null)
                    return false;
            } else if (!nodeList.equals(other.nodeList))
                return false;
            return true;
        }

    }

}

