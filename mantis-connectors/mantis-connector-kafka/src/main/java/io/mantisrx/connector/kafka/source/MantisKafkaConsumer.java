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

package io.mantisrx.connector.kafka.source;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.connector.kafka.source.assignor.StaticPartitionAssignor;
import io.mantisrx.connector.kafka.source.assignor.StaticPartitionAssignorImpl;
import io.mantisrx.connector.kafka.source.checkpoint.strategy.CheckpointStrategy;
import io.mantisrx.connector.kafka.source.checkpoint.strategy.CheckpointStrategyFactory;
import io.mantisrx.connector.kafka.source.checkpoint.strategy.CheckpointStrategyOptions;
import io.mantisrx.connector.kafka.source.checkpoint.trigger.CheckpointTrigger;
import io.mantisrx.connector.kafka.source.checkpoint.trigger.CheckpointTriggerFactory;
import io.mantisrx.connector.kafka.source.metrics.ConsumerMetrics;
import io.mantisrx.runtime.Context;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;


public class MantisKafkaConsumer<S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantisKafkaConsumer.class);

    private final int consumerId;
    private final KafkaConsumer<String, byte[]> consumer;
    private final CheckpointStrategy<S> strategy;
    private final CheckpointTrigger trigger;
    private final ConsumerMetrics consumerMetrics;
    private final TopicPartitionStateManager partitionStateManager;
    private final AtomicLong pollTimestamp = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong pollReturnedDataTimestamp = new AtomicLong(System.currentTimeMillis());
    private volatile Subscription metricSubscription = null;


    public MantisKafkaConsumer(final int consumerId,
                               final KafkaConsumer<String, byte[]> consumer,
                               final TopicPartitionStateManager partitionStateManager,
                               final CheckpointStrategy<S> strategy,
                               final CheckpointTrigger trigger,
                               final ConsumerMetrics metrics) {
        this.consumerId = consumerId;
        this.consumerMetrics = metrics;
        this.consumer = consumer;
        this.partitionStateManager = partitionStateManager;
        this.strategy = strategy;
        this.trigger = trigger;
        setupMetricPublish();
    }

    private void setupMetricPublish() {
        if (metricSubscription == null) {
            this.metricSubscription = Observable.interval(1, TimeUnit.SECONDS).subscribe((tick) -> {
                consumerMetrics.recordTimeSinceLastPollMs(timeSinceLastPollMs());
                consumerMetrics.recordTimeSinceLastPollWithDataMs(timeSinceLastPollWithDataMs());
            });
        }
    }

    public int getConsumerId() {
        return consumerId;
    }

    public KafkaConsumer<String, byte[]> getConsumer() {
        return consumer;
    }

    public CheckpointStrategy<S> getStrategy() {
        return strategy;
    }

    public CheckpointTrigger getTrigger() {
        return trigger;
    }

    public TopicPartitionStateManager getPartitionStateManager() {
        return partitionStateManager;
    }

    public long timeSinceLastPollMs() {
        return (System.currentTimeMillis() - pollTimestamp.get());
    }

    public long timeSinceLastPollWithDataMs() {
        return (System.currentTimeMillis() - pollReturnedDataTimestamp.get());
    }

    public ConsumerMetrics getConsumerMetrics() {
        return consumerMetrics;
    }

    public void close() {
        if (metricSubscription != null && !metricSubscription.isUnsubscribed()) {
            metricSubscription.unsubscribe();
        }

        if (trigger.isActive()) {
            final Set<TopicPartition> partitions = consumer.assignment();
            LOGGER.warn("clearing partition state when closing consumer {}, partitions {}", this.toString(), partitions.toString());
            partitions.stream().forEach(tp -> partitionStateManager.resetCounters(tp));
            consumer.close();
            trigger.shutdown();
        }
    }

    /**
     * {@link KafkaConsumer#poll(Duration)} ()}
     */
    public ConsumerRecords<String, byte[]> poll(final long consumerPollTimeoutMs) {
        final long now = System.currentTimeMillis();
        pollTimestamp.set(now);
        final ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.ofMillis(consumerPollTimeoutMs));
        if (consumerRecords.count() > 0) {
            pollReturnedDataTimestamp.set(now);
        }
        return consumerRecords;
    }

    /**
     * {@link KafkaConsumer#assignment()}
     */
    public Set<TopicPartition> assignment() {
        return consumer.assignment();
    }

    /**
     * {@link KafkaConsumer#listTopics()}
     */
    public Map<String, List<PartitionInfo>> listTopics() {
        return consumer.listTopics();
    }

    /**
     * {@link KafkaConsumer#position(TopicPartition)} ()}
     */
    public long position(TopicPartition partition) {
        return consumer.position(partition);
    }

    /**
     * {@link KafkaConsumer#seekToBeginning(Collection)} ()}
     */
    public void seekToBeginning(TopicPartition... partitions) {
        consumer.seekToBeginning(Arrays.asList(partitions));
    }

    /**
     * {@link KafkaConsumer#pause(Collection)} ()}
     */
    public void pause(TopicPartition... partitions) {
        LOGGER.debug("pausing {} partitions", partitions.length);
        consumer.pause(Arrays.asList(partitions));
        consumerMetrics.incrementPausePartitionCount();
    }

    /**
     * {@link KafkaConsumer#resume(Collection)} ()}
     */
    public void resume(TopicPartition... partitions) {
        try {
            LOGGER.debug("resuming {} partitions", partitions.length);
            consumer.resume(Arrays.asList(partitions));
            consumerMetrics.incrementResumePartitionCount();
        } catch (IllegalStateException e) {
            LOGGER.warn("resuming partitions failed", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantisKafkaConsumer that = (MantisKafkaConsumer) o;
        return consumerId == that.consumerId &&
            consumer.equals(that.consumer) &&
            strategy.equals(that.strategy) &&
            trigger.equals(that.trigger) &&
            consumerMetrics.equals(that.consumerMetrics) &&
            partitionStateManager.equals(that.partitionStateManager);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerId, consumer, strategy, trigger, consumerMetrics, partitionStateManager);
    }

    @Override
    public String toString() {
        return "MantisKafkaConsumer{" +
            "consumerId=" + consumerId +
            ", consumer=" + consumer +
            ", strategy=" + strategy +
            ", trigger=" + trigger +
            '}';
    }

    static class Builder {

        private Context context;
        private int consumerIndex;
        private int totalNumConsumersForJob;
        private Registry registry;
        private MantisKafkaSourceConfig kafkaSourceConfig;
        private static final AtomicInteger consumerId = new AtomicInteger(0);
        private final StaticPartitionAssignor staticPartitionAssignor = new StaticPartitionAssignorImpl();


        public Builder withContext(Context context) {
            this.context = context;
            return this;
        }

        public Builder withKafkaSourceConfig(MantisKafkaSourceConfig kafkaSourceConfig) {
            this.kafkaSourceConfig = kafkaSourceConfig;
            return this;
        }

        public Builder withConsumerIndex(int consumerIndex) {
            this.consumerIndex = consumerIndex;
            return this;
        }

        public Builder withTotalNumConsumersForJob(int totalNumConsumersForJob) {
            this.totalNumConsumersForJob = totalNumConsumersForJob;
            return this;
        }

        public Builder withRegistry(Registry registry) {
            this.registry = registry;
            return this;
        }

        private void doStaticPartitionAssignment(final KafkaConsumer<String, byte[]> consumer,
                                                 final ConsumerRebalanceListener rebalanceListener,
                                                 final int consumerIndex,
                                                 final int totalNumConsumers,
                                                 final Map<String, Integer> topicPartitionCounts,
                                                 final Registry registry) {
            if (totalNumConsumers <= 0) {
                LOGGER.error("total num consumers {} is invalid", totalNumConsumers);
                context.completeAndExit();
                return;
            }
            if (consumerIndex < 0 || consumerIndex >= totalNumConsumers) {
                LOGGER.error("consumerIndex {} is invalid (numConsumers: {})", consumerIndex, totalNumConsumers);
                context.completeAndExit();
                return;
            }

            final List<TopicPartition> topicPartitions = staticPartitionAssignor.assignPartitionsToConsumer(consumerIndex, topicPartitionCounts, totalNumConsumers);
            if (topicPartitions.isEmpty()) {
                LOGGER.error("topic partitions to assign list is empty");
                throw new RuntimeException("static partition assignment is enabled and no topic partitions were assigned, please check numPartitionsPerTopic job param is set correctly and the job has num(kafka consumer) <= num(partition)");
            } else {
                LOGGER.info("Statically assigned topic partitions(): {}", topicPartitions);
                topicPartitions.forEach(tp ->
                                            registry.gauge("staticPartitionAssigned",
                                                           "topic", tp.topic(), "partition", String.valueOf(tp.partition())).set(1.0));
                consumer.assign(topicPartitions);
                // reuse onPartitionsAssigned() so the consumer can seek to checkpoint'ed offset from offset store
                rebalanceListener.onPartitionsAssigned(topicPartitions);
            }
        }

        public MantisKafkaConsumer<?> build() {
            Preconditions.checkNotNull(context, "context");
            Preconditions.checkNotNull(kafkaSourceConfig, "kafkaSourceConfig");
            Preconditions.checkNotNull(registry, "registry");
            Preconditions.checkArg(consumerIndex >= 0, "consumerIndex must be greater than or equal to 0");
            Preconditions.checkArg(totalNumConsumersForJob > 0, "total number of consumers for job must be greater than 0");

            final int kafkaConsumerId = consumerId.incrementAndGet();

            Map<String, Object> consumerProps = kafkaSourceConfig.getConsumerConfig().getConsumerProperties();

            final String clientId = String.format("%s-%d-%d", context.getJobId(), context.getWorkerInfo().getWorkerNumber(), kafkaConsumerId);
            consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

            // hard-coding key to String type and value to byte[]
            final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);

            final TopicPartitionStateManager partitionStateManager = new TopicPartitionStateManager(registry, clientId, kafkaSourceConfig.getRetryCheckpointCheckDelayMs());
            final ConsumerMetrics metrics = new ConsumerMetrics(registry, kafkaConsumerId, context);
            final CheckpointStrategy<?> strategy = CheckpointStrategyFactory.getNewInstance(context, consumer, kafkaSourceConfig.getCheckpointStrategy(), metrics);

            if (kafkaSourceConfig.getStaticPartitionAssignmentEnabled()) {
                final KafkaConsumerRebalanceListener kafkaConsumerRebalanceListener = new KafkaConsumerRebalanceListener(consumer, partitionStateManager, strategy);
                kafkaSourceConfig.getTopicPartitionCounts().ifPresent(topicPartitionCounts -> {
                    doStaticPartitionAssignment(consumer, kafkaConsumerRebalanceListener, consumerIndex, totalNumConsumersForJob, topicPartitionCounts, registry);
                });
            } else {
                if (kafkaSourceConfig.getCheckpointStrategy() != CheckpointStrategyOptions.NONE) {
                    consumer.subscribe(kafkaSourceConfig.getTopics(),
                                       new KafkaConsumerRebalanceListener(consumer, partitionStateManager, strategy));
                } else {
                    consumer.subscribe(kafkaSourceConfig.getTopics());
                }
            }
            final CheckpointTrigger trigger = CheckpointTriggerFactory.getNewInstance(kafkaSourceConfig);
            return new MantisKafkaConsumer<>(kafkaConsumerId, consumer, partitionStateManager, strategy, trigger, metrics);
        }

    }
}
