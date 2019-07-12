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

package io.mantisrx.connector.kafka.source.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.mantisrx.runtime.Context;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


public class ConsumerMetrics {
    private static final String METRICS_PREFIX = "MantisKafkaConsumer_";
    private static final String METRIC_KAFKA_IN_COUNT = "kafkaInCount";
    private static final String METRIC_KAFKA_PROCESSED_COUNT = "kafkaProcessedCount";
    private static final String METRIC_KAFKA_ERROR_COUNT = "kafkaErrorCount";
    private static final String METRIC_KAFKA_WAIT_FOR_DATA_COUNT = "kafkaWaitForDataCount";
    private static final String METRIC_KAFKA_COMMIT_COUNT = "kafkaCommitCount";
    private static final String METRIC_CHECKPOINT_DELAY = "checkpointDelay";
    private static final String METRIC_PARSE_FAILURE_COUNT = "parseFailureCount";
    private static final String METRIC_KAFKA_MSG_VALUE_NULL_COUNT = "kafkaMessageValueNull";
    private static final String METRIC_TIME_SINCE_LAST_POLL_MS = "timeSinceLastPollMs";
    private static final String METRIC_TIME_SINCE_LAST_POLL_WITH_DATA_MS = "timeSinceLastPollWithDataMs";

    private static final String METRIC_KAFKA_PAUSE_PARTITIONS = "kafkaPausePartitions";
    private static final String METRIC_KAFKA_RESUME_PARTITIONS = "kafkaResumePartitions";

    private final Registry registry;
    private final List<Tag> commonTags;
    private final Counter kafkaInCount;
    private final Counter kafkaProcessedCount;
    private final Counter kafkaErrorCount;
    private final Counter kafkaWaitForDataCount;
    private final Counter kafkaCommitCount;
    private final Counter parseFailureCount;
    private final Counter kafkaPausePartitions;
    private final Counter kafkaResumePartitions;
    private final Counter kafkaMsgValueNullCount;
    private final Gauge checkpointDelay;
    private final Gauge timeSinceLastPollMs;
    private final Gauge timeSinceLastPollWithDataMs;
    private final ConcurrentMap<TopicPartition, Gauge> committedOffsets = new ConcurrentHashMap<>();
    private final ConcurrentMap<TopicPartition, Gauge> readOffsets = new ConcurrentHashMap<>();

    public ConsumerMetrics(final Registry registry, final int consumerId, final Context context) {
        this.registry = registry;
        this.commonTags = createCommonTags(context, consumerId);
        this.kafkaErrorCount = registry.counter(createId(METRIC_KAFKA_ERROR_COUNT));
        this.kafkaInCount = registry.counter(createId(METRIC_KAFKA_IN_COUNT));
        this.kafkaProcessedCount = registry.counter(createId(METRIC_KAFKA_PROCESSED_COUNT));
        this.kafkaWaitForDataCount = registry.counter(createId(METRIC_KAFKA_WAIT_FOR_DATA_COUNT));
        this.kafkaCommitCount = registry.counter(createId(METRIC_KAFKA_COMMIT_COUNT));
        this.checkpointDelay = registry.gauge(createId(METRIC_CHECKPOINT_DELAY));
        this.timeSinceLastPollMs = registry.gauge(createId(METRIC_TIME_SINCE_LAST_POLL_MS));
        this.timeSinceLastPollWithDataMs = registry.gauge(createId(METRIC_TIME_SINCE_LAST_POLL_WITH_DATA_MS));
        this.parseFailureCount = registry.counter(createId(METRIC_PARSE_FAILURE_COUNT));
        this.kafkaPausePartitions = registry.counter(createId(METRIC_KAFKA_PAUSE_PARTITIONS));
        this.kafkaResumePartitions = registry.counter(createId(METRIC_KAFKA_RESUME_PARTITIONS));
        this.kafkaMsgValueNullCount = registry.counter(createId(METRIC_KAFKA_MSG_VALUE_NULL_COUNT));
    }

    private List<Tag> createCommonTags(final Context context, final int consumerId) {
        return Arrays.asList(Tag.of("mantisWorkerNum", Integer.toString(context.getWorkerInfo().getWorkerNumber())),
                Tag.of("mantisWorkerIndex", Integer.toString(context.getWorkerInfo().getWorkerIndex())),
                Tag.of("mantisJobName", context.getWorkerInfo().getJobClusterName()),
                Tag.of("mantisJobId", context.getJobId()),
                Tag.of("consumerId", String.valueOf(consumerId)));
    }

    private Id createId(final String metricName) {
        return registry.createId(METRICS_PREFIX + metricName, commonTags);
    }

    public void recordCheckpointDelay(final long value) {
        checkpointDelay.set(value);
    }
    public void recordTimeSinceLastPollMs(long value) {
        timeSinceLastPollMs.set(value);
    }

    public void recordTimeSinceLastPollWithDataMs(long value) {
        timeSinceLastPollWithDataMs.set(value);
    }

    public void incrementInCount() {
        kafkaInCount.increment();
    }

    public void incrementProcessedCount() {
        kafkaProcessedCount.increment();
    }

    public void incrementErrorCount() {
        kafkaErrorCount.increment();
    }

    public void incrementWaitForDataCount() {
        kafkaWaitForDataCount.increment();
    }

    public void incrementCommitCount() {
        kafkaCommitCount.increment();
    }

    public void incrementParseFailureCount() {
        parseFailureCount.increment();
    }

    public void incrementPausePartitionCount() {
        kafkaPausePartitions.increment();
    }
    public void incrementResumePartitionCount() {
        kafkaResumePartitions.increment();
    }

    public void incrementKafkaMessageValueNullCount() {
        kafkaMsgValueNullCount.increment();
    }

    public void recordCommittedOffset(final Map<TopicPartition, OffsetAndMetadata> checkpoint) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : checkpoint.entrySet()) {
            final TopicPartition tp = entry.getKey();
            if (!committedOffsets.containsKey(tp)) {
                ArrayList<Tag> tags = new ArrayList<>(commonTags);
                tags.add(Tag.of("topic", tp.topic()));
                tags.add(Tag.of("partition", String.valueOf(tp.partition())));
                Gauge gauge = registry.gauge("committedOffsets", tags);
                committedOffsets.putIfAbsent(tp, gauge);
            }
            committedOffsets.get(tp).set(entry.getValue().offset());
        }
    }

    public void recordReadOffset(final TopicPartition tp, final long offset) {
        if (!readOffsets.containsKey(tp)) {
            ArrayList<Tag> tags = new ArrayList<>(commonTags);
            tags.add(Tag.of("topic", tp.topic()));
            tags.add(Tag.of("partition", String.valueOf(tp.partition())));
            Gauge gauge = registry.gauge("minReadOffsets", tags);
            readOffsets.putIfAbsent(tp, gauge);
        }
        readOffsets.get(tp).set(offset);
    }
}
