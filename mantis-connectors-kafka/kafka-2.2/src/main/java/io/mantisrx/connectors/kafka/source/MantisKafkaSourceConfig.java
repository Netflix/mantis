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

import static io.mantisrx.connectors.kafka.source.MantisKafka22ConsumerConfig.DEFAULT_CHECKPOINT_INTERVAL_MS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Splitter;
import io.mantisrx.connectors.kafka.KafkaSourceParameters;
import io.mantisrx.connectors.kafka.source.checkpoint.strategy.CheckpointStrategyOptions;
import io.mantisrx.connectors.kafka.source.serde.ParserType;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MantisKafkaSourceConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantisKafkaSourceConfig.class);
    public static final int DEFAULT_CONSUMER_POLL_TIMEOUT_MS = 100;
    public static final int DEFAULT_RETRY_CHECKPOINT_CHECK_DELAY_MS = 20;
    public static final boolean DEFAULT_ENABLE_STATIC_PARTITION_ASSIGN = false;
    public static final int CONSUMER_RECORD_OVERHEAD_BYTES = 100;
    public static final int DEFAULT_MAX_BYTES_IN_PROCESSING = 128_000_000;
    public static final int DEFAULT_NUM_KAFKA_CONSUMER_PER_WORKER = 1;
    public static final boolean DEFAULT_PARSE_MSG_IN_SOURCE = true;

    private final List<String> topics;
    private final int numConsumerInstances;
    private final int consumerPollTimeoutMs;
    private final int maxBytesInProcessing;
    private final String messageParserType;
    private final String checkpointStrategy;
    private final Boolean parseMessageInSource;
    private final int retryCheckpointCheckDelayMs;
    private final int checkpointIntervalMs;
    private final Boolean staticPartitionAssignmentEnabled;
    private final Optional<Map<String, Integer>> topicPartitionCounts;
    private final MantisKafka22ConsumerConfig consumerConfig;

    public MantisKafkaSourceConfig(Context context) {
        final Parameters parameters = context.getParameters();
        final String topicStr = (String) parameters.get(KafkaSourceParameters.TOPIC);
        this.topics = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(topicStr);
        this.numConsumerInstances = (int) parameters.get(KafkaSourceParameters.NUM_KAFKA_CONSUMER_PER_WORKER, DEFAULT_NUM_KAFKA_CONSUMER_PER_WORKER);
        this.consumerPollTimeoutMs = (int) parameters.get(KafkaSourceParameters.CONSUMER_POLL_TIMEOUT_MS, DEFAULT_CONSUMER_POLL_TIMEOUT_MS);
        this.maxBytesInProcessing = (int) parameters.get(KafkaSourceParameters.MAX_BYTES_IN_PROCESSING, DEFAULT_MAX_BYTES_IN_PROCESSING);
        this.messageParserType = (String) parameters.get(KafkaSourceParameters.PARSER_TYPE, ParserType.SIMPLE_JSON.getPropName());
        this.checkpointStrategy = (String) parameters.get(KafkaSourceParameters.CHECKPOINT_STRATEGY, CheckpointStrategyOptions.NONE);
        this.parseMessageInSource = (boolean) parameters.get(KafkaSourceParameters.PARSE_MSG_IN_SOURCE, DEFAULT_PARSE_MSG_IN_SOURCE);
        this.retryCheckpointCheckDelayMs = (int) parameters.get(KafkaSourceParameters.RETRY_CHECKPOINT_CHECK_DELAY_MS, DEFAULT_RETRY_CHECKPOINT_CHECK_DELAY_MS);
        this.checkpointIntervalMs = (int) parameters.get(KafkaSourceParameters.CHECKPOINT_INTERVAL_MS, DEFAULT_CHECKPOINT_INTERVAL_MS);
        this.staticPartitionAssignmentEnabled = (boolean) parameters.get(KafkaSourceParameters.ENABLE_STATIC_PARTITION_ASSIGN, DEFAULT_ENABLE_STATIC_PARTITION_ASSIGN);
        if (staticPartitionAssignmentEnabled) {
            final String topicPartitionsStr = (String) parameters.get(KafkaSourceParameters.TOPIC_PARTITION_COUNTS, "");
            this.topicPartitionCounts = Optional.ofNullable(getTopicPartitionCounts(topicPartitionsStr, topics));
        } else {
            this.topicPartitionCounts = Optional.empty();
        }
        consumerConfig = new MantisKafka22ConsumerConfig(context);
        LOGGER.info("checkpointStrategy: {} numConsumerInstances: {} topics: {} consumerPollTimeoutMs: {} retryCheckpointCheckDelayMs {} consumer config: {}",
                    checkpointStrategy, numConsumerInstances, topics, consumerPollTimeoutMs, retryCheckpointCheckDelayMs, consumerConfig.values().toString());
    }

    private Map<String, Integer> getTopicPartitionCounts(String topicPartitionsStr, List<String> topicList) {
        final List<String> topicPartitionCountList = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(topicPartitionsStr);
        final Map<String, Integer> topicPartitionCounts = new HashMap<>();
        // parse topic partition counts only if Static partition assignment is enabled
        for (String tp : topicPartitionCountList) {
            final String[] topicPartitionCount = tp.split(":");
            if (topicPartitionCount.length == 2) {
                final String topic = topicPartitionCount[0];
                if (topicList.contains(topic)) {
                    topicPartitionCounts.put(topic, Integer.parseInt(topicPartitionCount[1]));
                } else {
                    final String errorMsg = String.format("topic %s specified in Job Parameter '%s' does not match topics specified for Job Parameter '%s'",
                                                          topic, KafkaSourceParameters.TOPIC_PARTITION_COUNTS, KafkaSourceParameters.TOPIC);
                    LOGGER.error(errorMsg);
                    throw new RuntimeException(errorMsg);
                }
            } else {
                final String errorMsg = String.format("failed to parse topic partition count string %s", tp);
                LOGGER.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }

        // validate all topics have partition counts specified
        final Set<String> partitionCountTopics = topicPartitionCounts.keySet();
        if (!partitionCountTopics.containsAll(topicList) ||
            !topicList.containsAll(partitionCountTopics)) {
            final String errorMsg = String.format("topics '%s' specified for Job Parameter '%s' don't match topics '%s' specified for Job Parameter '%s'",
                                                  partitionCountTopics, KafkaSourceParameters.TOPIC_PARTITION_COUNTS, topicList, KafkaSourceParameters.TOPIC);
            LOGGER.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        LOGGER.info("enableStaticPartitionAssignment: {} [ topic partition counts: {} ]", staticPartitionAssignmentEnabled, topicPartitionCounts);
        return topicPartitionCounts;
    }

    public List<String> getTopics() {
        return topics;
    }

    public int getNumConsumerInstances() {
        return numConsumerInstances;
    }

    public int getConsumerPollTimeoutMs() {
        return consumerPollTimeoutMs;
    }

    public int getMaxBytesInProcessing() {
        return maxBytesInProcessing;
    }

    public String getMessageParserType() {
        return messageParserType;
    }

    public String getCheckpointStrategy() {
        return checkpointStrategy;
    }

    public Boolean getParseMessageInSource() {
        return parseMessageInSource;
    }

    public int getRetryCheckpointCheckDelayMs() {
        return retryCheckpointCheckDelayMs;
    }

    public int getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public Boolean getStaticPartitionAssignmentEnabled() {
        return staticPartitionAssignmentEnabled;
    }

    public Optional<Map<String, Integer>> getTopicPartitionCounts() {
        return topicPartitionCounts;
    }

    public MantisKafka22ConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }
}
