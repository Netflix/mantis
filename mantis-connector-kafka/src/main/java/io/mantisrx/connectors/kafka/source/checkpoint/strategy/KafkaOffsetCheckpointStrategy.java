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

package io.mantisrx.connectors.kafka.source.checkpoint.strategy;

import io.mantisrx.connectors.kafka.source.metrics.ConsumerMetrics;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mantisrx.runtime.Context;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Leverages the default Kafka facilities to commit offsets to Kafka using {@link KafkaConsumer#commitSync(Map) commitSync(Map)}.
 */
public class KafkaOffsetCheckpointStrategy implements CheckpointStrategy<OffsetAndMetadata> {

    private static Logger logger = LoggerFactory.getLogger(KafkaOffsetCheckpointStrategy.class);

    private final KafkaConsumer<?, ?> consumer;
    private final ConsumerMetrics consumerMetrics;

    public KafkaOffsetCheckpointStrategy(KafkaConsumer<?, ?> consumer, ConsumerMetrics metrics) {
        this.consumer = consumer;
        this.consumerMetrics = metrics;
    }

    @Override
    public void init(Map<String, String> properties) {
    }

    @Override
    public boolean persistCheckpoint(final Map<TopicPartition, OffsetAndMetadata> checkpoint) {
        if (!checkpoint.isEmpty()) {
            try {
                logger.debug("committing offsets {}", checkpoint.toString());
                consumer.commitSync(checkpoint);
                consumerMetrics.recordCommittedOffset(checkpoint);
            } catch (InvalidOffsetException ioe) {
                logger.warn("failed to commit offsets " + checkpoint.toString() + " will seek to beginning", ioe);
                final Set<TopicPartition> topicPartitionSet = ioe.partitions();
                for (TopicPartition tp : topicPartitionSet) {
                    logger.info("partition " + tp.toString() + " consumer position " + consumer.position(tp));
                }
                consumer.seekToBeginning(ioe.partitions());
            } catch (KafkaException cfe) {
                // should not be retried
                logger.warn("unrecoverable exception on commit offsets " + checkpoint.toString(), cfe);
                return false;
            }
        }
        return true;
    }

    @Override
    public Optional<OffsetAndMetadata> loadCheckpoint(TopicPartition tp) {
        logger.trace("rely on default kafka protocol to seek to last committed offset");
        return Optional.empty();
    }

    @Override
    public void init(Context context) {
        // no-op
    }

    @Override
    public Map<TopicPartition, Optional<OffsetAndMetadata>> loadCheckpoints(List<TopicPartition> tpList) {
        Map<TopicPartition, Optional<OffsetAndMetadata>> mp = new HashMap<>();
        for (TopicPartition tp : tpList) {
            mp.put(tp, loadCheckpoint(tp));
        }
        return mp;
    }

    @Override
    public String type() {
        return CheckpointStrategyOptions.OFFSETS_ONLY_DEFAULT;
    }
}
