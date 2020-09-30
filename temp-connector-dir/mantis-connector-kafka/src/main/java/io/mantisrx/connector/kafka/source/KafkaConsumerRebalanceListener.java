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

package io.mantisrx.connector.kafka.source;

import java.util.Collection;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mantisrx.connector.kafka.source.checkpoint.strategy.CheckpointStrategy;


public class KafkaConsumerRebalanceListener<S> implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerRebalanceListener.class);

    private final KafkaConsumer<?, ?> consumer;
    private final TopicPartitionStateManager partitionStateManager;
    private final CheckpointStrategy<S> checkpointStrategy;

    public KafkaConsumerRebalanceListener(final KafkaConsumer<?, ?> consumer,
                                          final TopicPartitionStateManager partitionStateManager,
                                          final CheckpointStrategy<S> checkpointStrategy) {
        this.consumer = consumer;
        this.partitionStateManager = partitionStateManager;
        this.checkpointStrategy = checkpointStrategy;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // When partitions are revoked, clear all partition state. We don't try to checkpoint here as we can be stuck indefinitely if the processing is slow and
        // we try to wait for all acks to create a checkpoint and commit the offsets/state to data store.
        LOGGER.info("partitions revoked, resetting partition state: {}", partitions.toString());
        partitions.stream().forEach(tp -> partitionStateManager.resetCounters(tp));
    }

    /**
     * Assumption is onPartitionsRevoked will always be called before onPartitionsAssigned
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.info("new partitions assigned: {}", partitions.toString());
        try {
            for (TopicPartition tp : partitions) {
                Optional<S> checkpointState = checkpointStrategy.loadCheckpoint(tp);
                checkpointState
                    .filter(x -> x instanceof OffsetAndMetadata)
                    .map(OffsetAndMetadata.class::cast)
                    .ifPresent(oam -> {
                        long offset = oam.offset();
                        LOGGER.info("seeking consumer to checkpoint'ed offset {} for partition {} on assignment", offset, tp);
                        try {
                            consumer.seek(tp, offset);
                        } catch (Exception e) {
                            LOGGER.error("caught exception seeking consumer to offset {} on topic partition {}", offset, tp, e);
                        }
                    });
            }
        } catch (Exception e) {
            LOGGER.error("caught exception on partition assignment {}", partitions, e);
        }
    }
}
