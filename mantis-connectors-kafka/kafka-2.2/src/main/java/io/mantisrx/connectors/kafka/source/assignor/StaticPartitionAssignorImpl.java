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

package io.mantisrx.connectors.kafka.source.assignor;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Is invoked during initialization of the KafkaSource22 if Static partitioning ins enabled.
 */

public class StaticPartitionAssignorImpl implements StaticPartitionAssignor {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticPartitionAssignorImpl.class);

    /**
     * Does a simple round robin assignment of each TopicName-PartitionNumber combination to the list of consumers
     * Returns only the assignments for the current consumer.
     *
     * @param consumerIndex        Current workers consumerIndex
     * @param topicPartitionCounts Map of topic -> no of partitions
     * @param totalNumConsumers    Total number of consumers
     *
     * @return
     */
    @Override
    public List<TopicPartition> assignPartitionsToConsumer(int consumerIndex, Map<String, Integer> topicPartitionCounts, int totalNumConsumers) {
        Objects.requireNonNull(topicPartitionCounts, "TopicPartitionCount Map cannot be null");

        if (consumerIndex < 0) {
            throw new IllegalArgumentException("Consumer Index cannot be negative " + consumerIndex);
        }
        if (totalNumConsumers < 0) {
            throw new IllegalArgumentException("Total Number of consumers cannot be negative " + totalNumConsumers);
        }

        if (consumerIndex >= totalNumConsumers) {
            throw new IllegalArgumentException("Consumer Index " + consumerIndex + " cannot be greater than or equal to Total Number of consumers " + totalNumConsumers);

        }

        List<TopicPartition> topicPartitions = new ArrayList<>();
        int currConsumer = 0;

        for (Map.Entry<String, Integer> topicPartitionCount : topicPartitionCounts.entrySet()) {

            final String topic = topicPartitionCount.getKey();
            final Integer numPartitions = topicPartitionCount.getValue();
            if (numPartitions <= 0) {
                LOGGER.warn("Number of partitions is " + numPartitions + " for Topic " + topic + " skipping");
                continue;
            }
            for (int i = 0; i < numPartitions; i++) {
                if (currConsumer == totalNumConsumers) {
                    currConsumer = 0;
                }
                if (currConsumer == consumerIndex) {
                    topicPartitions.add(new TopicPartition(topic, i));

                }
                currConsumer++;
            }
        }

        return topicPartitions;

    }
}
