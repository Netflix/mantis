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

package io.mantisrx.connectors.kafka;

public class KafkaSourceParameters {

    public static final String CHECKPOINT_STRATEGY = "checkpointStrategy";
    public static final String CONSUMER_POLL_TIMEOUT_MS = "consumerPollTimeoutMs";
    public static final String NUM_KAFKA_CONSUMER_PER_WORKER = "numKafkaConsumerPerWorker";
    public static final String TOPIC = "topic";
    public static final String KAFKA_VIP = "kafkaVip";
    public static final String MAX_BYTES_IN_PROCESSING = "maxBytesInProcessing";
    public static final String PARSER_TYPE = "messageParserType";
    public static final String PARSE_MSG_IN_SOURCE = "parseMessageInKafkaConsumerThread";
    public static final String RETRY_CHECKPOINT_CHECK_DELAY_MS = "retryCheckpointCheckDelayMs";
    public static final String CHECKPOINT_INTERVAL_MS = "checkpointIntervalMs";

    // Enable static partition assignment, this disables Kafka's default consumer group management
    public static final String ENABLE_STATIC_PARTITION_ASSIGN = "enableStaticPartitionAssign";
    // Number of partitions per topic, used only when Static Partition assignment is enabled
    public static final String TOPIC_PARTITION_COUNTS = "numPartitionsPerTopic";

}
