/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connectors.kafka.source;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.netty.util.internal.ConcurrentSet;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;


public class TopicPartitionStateManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionStateManager.class);

    // Default to 20ms delay between retries for checkpoint ready
    private final int checkpointReadyCheckDelayMs;

    private final Counter waitingForAckCount;

    public TopicPartitionStateManager(Registry registry, String kafkaClientId, int checkpointReadyCheckDelayMs) {
        this.checkpointReadyCheckDelayMs = checkpointReadyCheckDelayMs;
        this.waitingForAckCount = registry.counter("waitingOnAck", "client-id", kafkaClientId);
    }

    public static final long DEFAULT_LAST_READ_OFFSET = 0;

    private class State {

        private final AtomicLong lastReadOffset = new AtomicLong(DEFAULT_LAST_READ_OFFSET);
        private final ConcurrentSet<Long> unAckedOffsets = new ConcurrentSet<>();
    }

    private final ConcurrentMap<TopicPartition, State> partitionState = new ConcurrentHashMap<>();

    /**
     * Track the message with this offset as read from Kafka but waiting on acknowledgement from the processing stage.
     *
     * @param tp     TopicPartition the message was read from
     * @param offset kafka offset for the message
     */
    public void recordMessageRead(final TopicPartition tp, final long offset) {
        // add to set
        if (!partitionState.containsKey(tp)) {
            partitionState.putIfAbsent(tp, new State());
        }
        partitionState.get(tp).unAckedOffsets.add(offset);
        partitionState.get(tp).lastReadOffset.set(offset);
    }

    /**
     * Records the message identified by this offset has been processed and ack'ed by the processing stage.
     *
     * @param tp     TopicPartition the message was read from
     * @param offset kafka offset for the message
     */
    public void recordMessageAck(final TopicPartition tp, final long offset) {
        // remove from set
        if (!partitionState.containsKey(tp)) {
            return;
        }
        partitionState.get(tp).unAckedOffsets.remove(offset);
    }

    /**
     * Get last read offset from this topic partition.
     *
     * @param tp TopicPartition
     *
     * @return last offset read from give TopicPartition
     */
    public Optional<Long> getLastOffset(final TopicPartition tp) {
        if (!partitionState.containsKey(tp)) {
            return Optional.empty();
        }
        return Optional.of(partitionState.get(tp).lastReadOffset.get());
    }

    private boolean allMessagesAcked(final TopicPartition tp) {
        if (!partitionState.containsKey(tp)) {
            // no messages, no acks needed
            return true;
        }
        return partitionState.get(tp).unAckedOffsets.size() == 0;
    }

    public Map<TopicPartition, OffsetAndMetadata> createCheckpoint(final Collection<TopicPartition> partitions) {
        if (partitionState.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<TopicPartition, OffsetAndMetadata> checkpoint = new HashMap<>(partitions.size());

        for (TopicPartition tp : partitions) {
            while (!allMessagesAcked(tp)) {
                try {
                    waitingForAckCount.increment();
                    Thread.sleep(checkpointReadyCheckDelayMs);
                } catch (InterruptedException e) {
                    LOGGER.info("thread interrupted when creating checkpoint for {}", tp);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("thread interrupted when creating checkpoint", e);
                }
            }
            final State pState = partitionState.get(tp);
            final Optional<Long> lastOffset = Optional.ofNullable(pState != null ? pState.lastReadOffset.get() : null);
            if (lastOffset.isPresent() && lastOffset.get() != DEFAULT_LAST_READ_OFFSET) {
                checkpoint.put(tp, new OffsetAndMetadata(lastOffset.get() + 1, String.valueOf(System.currentTimeMillis())));
            }
        }
        return checkpoint;
    }

    /* reset partition counters */
    public void resetCounters(final TopicPartition tp) {
        if (!partitionState.containsKey(tp)) {
            return;
        }
        partitionState.get(tp).unAckedOffsets.clear();
        partitionState.get(tp).lastReadOffset.set(DEFAULT_LAST_READ_OFFSET);
    }

    /* reset all counters */
    public void resetCounters() {
        LOGGER.info("resetting all counters");
        if (partitionState.isEmpty()) {
            return;
        }
        partitionState.values().stream().forEach(state -> {
            state.unAckedOffsets.clear();
            state.lastReadOffset.set(DEFAULT_LAST_READ_OFFSET);
        });
    }
}
