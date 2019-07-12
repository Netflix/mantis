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

import com.netflix.spectator.api.Registry;
import io.mantisrx.connector.kafka.source.metrics.ConsumerMetrics;
import io.mantisrx.connector.kafka.source.serde.ParseException;
import io.mantisrx.connector.kafka.source.serde.Parser;
import io.mantisrx.connector.kafka.source.serde.ParserType;
import io.mantisrx.connector.kafka.KafkaData;
import io.mantisrx.connector.kafka.KafkaDataNotification;
import io.mantisrx.connector.kafka.source.checkpoint.strategy.CheckpointStrategy;
import io.mantisrx.connector.kafka.source.checkpoint.strategy.CheckpointStrategyOptions;
import io.mantisrx.connector.kafka.source.checkpoint.trigger.CheckpointTrigger;
import io.mantisrx.connector.kafka.KafkaSourceParameters;
import io.mantisrx.connector.kafka.KafkaAckable;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.BooleanParameter;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.InvalidRecordException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.observables.SyncOnSubscribe;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mantisrx.connector.kafka.source.MantisKafkaSourceConfig.CONSUMER_RECORD_OVERHEAD_BYTES;
import static io.mantisrx.connector.kafka.source.MantisKafkaSourceConfig.DEFAULT_ENABLE_STATIC_PARTITION_ASSIGN;
import static io.mantisrx.connector.kafka.source.MantisKafkaSourceConfig.DEFAULT_MAX_BYTES_IN_PROCESSING;
import static io.mantisrx.connector.kafka.source.MantisKafkaSourceConfig.DEFAULT_NUM_KAFKA_CONSUMER_PER_WORKER;
import static io.mantisrx.connector.kafka.source.MantisKafkaSourceConfig.DEFAULT_PARSE_MSG_IN_SOURCE;


/**
 * Mantis Kafka Source wraps a kafka v2.2.+ consumer with back pressure semantics, the consumer polls data from kafka
 * only as fast as the data is processed & ack'ed by the processing stage of the Mantis Job.
 * <p>
 * The {@value KafkaSourceParameters#NUM_KAFKA_CONSUMER_PER_WORKER} Job param decides the number of Kafka consumer instances spawned on each Mantis worker,
 * Each kafka consumer instance runs in their own thread and poll data from kafka as part of the same consumer group
 */
public class KafkaSource implements Source<KafkaAckable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

    private final AtomicBoolean done = new AtomicBoolean();

    private final Map<Integer, MantisKafkaConsumer<?>> idToConsumerMap = new HashMap<>();
    private final Registry registry;

    private final SerializedSubject<KafkaDataNotification, KafkaDataNotification> ackSubject =
        new SerializedSubject<>(PublishSubject.create());


    public KafkaSource(final Registry registry) {
        this.registry = registry;
    }

    private Observable<MantisKafkaConsumer<?>> createConsumers(final Context context,
                                                               final MantisKafkaSourceConfig kafkaSourceConfig,
                                                               final int totalNumWorkers) {

        final List<MantisKafkaConsumer<?>> consumers = new ArrayList<>();
        for (int i = 0; i < kafkaSourceConfig.getNumConsumerInstances(); i++) {
            final int consumerIndex = context.getWorkerInfo().getWorkerIndex() + (totalNumWorkers * i);
            MantisKafkaConsumer<?> mantisKafkaConsumer = new MantisKafkaConsumer.Builder()
                .withKafkaSourceConfig(kafkaSourceConfig)
                .withTotalNumConsumersForJob(totalNumWorkers * kafkaSourceConfig.getNumConsumerInstances())
                .withContext(context)
                .withConsumerIndex(consumerIndex)
                .withRegistry(registry)
                .build();
            idToConsumerMap.put(mantisKafkaConsumer.getConsumerId(), mantisKafkaConsumer);
            LOGGER.info("created consumer {}", mantisKafkaConsumer);
            consumers.add(mantisKafkaConsumer);
        }
        return Observable.from(consumers);
    }

    private int getPayloadSize(ConsumerRecord<String, byte[]> record) {
        return record.value().length + CONSUMER_RECORD_OVERHEAD_BYTES;
    }

    /**
     * Create an observable with back pressure semantics from the consumer records fetched using consumer.
     *
     * @param mantisKafkaConsumer non thread-safe KafkaConsumer
     * @param kafkaSourceConfig   configuration for the Mantis Kafka Source
     */
    private Observable<KafkaAckable> createBackPressuredConsumerObs(final MantisKafkaConsumer<?> mantisKafkaConsumer,
                                                                    final MantisKafkaSourceConfig kafkaSourceConfig) {
        CheckpointStrategy checkpointStrategy = mantisKafkaConsumer.getStrategy();
        final CheckpointTrigger trigger = mantisKafkaConsumer.getTrigger();
        final ConsumerMetrics consumerMetrics = mantisKafkaConsumer.getConsumerMetrics();
        final TopicPartitionStateManager partitionStateManager = mantisKafkaConsumer.getPartitionStateManager();
        int mantisKafkaConsumerId = mantisKafkaConsumer.getConsumerId();

        SyncOnSubscribe<Iterator<ConsumerRecord<String, byte[]>>, KafkaAckable> syncOnSubscribe = SyncOnSubscribe.createStateful(
            () -> {
                final ConsumerRecords<String, byte[]> records = mantisKafkaConsumer.poll(kafkaSourceConfig.getConsumerPollTimeoutMs());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("topic listing for consumer {}", mantisKafkaConsumer.listTopics());
                }
                LOGGER.info("consumer subscribed to topic-partitions {}", mantisKafkaConsumer.assignment());
                return records.iterator();
            },
            (consumerRecordIterator, observer) -> {
                Iterator<ConsumerRecord<String, byte[]>> it = consumerRecordIterator;
                final Set<TopicPartition> partitions = mantisKafkaConsumer.assignment();

                if (trigger.shouldCheckpoint()) {
                    long startTime = System.currentTimeMillis();

                    final Map<TopicPartition, OffsetAndMetadata> checkpoint =
                        partitionStateManager.createCheckpoint(partitions);
                    checkpointStrategy.persistCheckpoint(checkpoint);
                    long now = System.currentTimeMillis();
                    consumerMetrics.recordCheckpointDelay(now - startTime);
                    consumerMetrics.incrementCommitCount();
                    trigger.reset();
                }
                if (!done.get()) {
                    try {
                        if (!consumerRecordIterator.hasNext()) {
                            final ConsumerRecords<String, byte[]> consumerRecords =
                                mantisKafkaConsumer.poll(kafkaSourceConfig.getConsumerPollTimeoutMs());
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("poll returned {} records", consumerRecords.count());
                            }
                            it = consumerRecords.iterator();
                        }

                        if (it.hasNext()) {
                            final ConsumerRecord<String, byte[]> m = it.next();
                            final TopicPartition topicPartition = new TopicPartition(m.topic(), m.partition());

                            consumerMetrics.incrementInCount();
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("updating read offset to " + m.offset() + " read " + m.value());
                            }

                            if (m.value() != null) {
                                try {
                                    trigger.update(getPayloadSize(m));

                                    if (kafkaSourceConfig.getParseMessageInSource()) {
                                        final Parser parser = ParserType.parser(kafkaSourceConfig.getMessageParserType()).getParser();
                                        if (parser.canParse(m.value())) {
                                            final Map<String, Object> parsedKafkaValue = parser.parseMessage(m.value());
                                            final KafkaData kafkaData = new KafkaData(m, Optional.ofNullable(parsedKafkaValue), Optional.ofNullable(m.key()), mantisKafkaConsumerId);
                                            final KafkaAckable ackable = new KafkaAckable(kafkaData, ackSubject);
                                            // record offset consumed in TopicPartitionStateManager before onNext to avoid race condition with Ack being processed before the consume is recorded
                                            partitionStateManager.recordMessageRead(topicPartition, m.offset());
                                            consumerMetrics.recordReadOffset(topicPartition, m.offset());
                                            observer.onNext(ackable);
                                        } else {
                                            consumerMetrics.incrementParseFailureCount();
                                        }
                                    } else {
                                        final KafkaData kafkaData = new KafkaData(m, Optional.empty(), Optional.ofNullable(m.key()), mantisKafkaConsumerId);
                                        final KafkaAckable ackable = new KafkaAckable(kafkaData, ackSubject);
                                        // record offset consumed in TopicPartitionStateManager before onNext to avoid race condition with Ack being processed before the consume is recorded
                                        partitionStateManager.recordMessageRead(topicPartition, m.offset());
                                        consumerMetrics.recordReadOffset(topicPartition, m.offset());
                                        observer.onNext(ackable);
                                    }
                                } catch (ParseException pe) {
                                    consumerMetrics.incrementErrorCount();
                                    LOGGER.warn("failed to parse {}:{} message {}", m.topic(), m.partition(), m.value(), pe);
                                }
                            } else {
                                consumerMetrics.incrementKafkaMessageValueNullCount();
                            }
                        } else {
                            consumerMetrics.incrementWaitForDataCount();
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Reached head of partition, waiting for more data");
                            }
                            TimeUnit.MILLISECONDS.sleep(200);
                        }
                    } catch (TimeoutException toe) {
                        consumerMetrics.incrementWaitForDataCount();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Reached head of partition waiting for more data");
                        }
                    } catch (OffsetOutOfRangeException oore) {
                        LOGGER.warn("offsets out of range " + oore.partitions() + " will seek to beginning", oore);
                        final Set<TopicPartition> topicPartitionSet = oore.partitions();
                        for (TopicPartition tp : topicPartitionSet) {
                            LOGGER.info("partition {} consumer position {}", tp, mantisKafkaConsumer.position(tp));
                        }
                        mantisKafkaConsumer.seekToBeginning(oore.partitions().toArray(new TopicPartition[oore.partitions().size()]));
                    } catch (InvalidRecordException ire) {
                        consumerMetrics.incrementErrorCount();
                        LOGGER.warn("iterator error with invalid message. message will be dropped " + ire.getMessage());
                    } catch (KafkaException e) {
                        consumerMetrics.incrementErrorCount();
                        LOGGER.warn("Other Kafka exception, message will be dropped. " + e.getMessage());
                    } catch (InterruptedException ie) {
                        LOGGER.error("consumer interrupted", ie);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        consumerMetrics.incrementErrorCount();
                        LOGGER.warn("caught exception", e);
                    }
                } else {
                    mantisKafkaConsumer.close();
                }
                return it;
            },
            consumerRecordIterator -> {
                LOGGER.info("closing Kafka consumer on unsubscribe" + mantisKafkaConsumer.toString());
                mantisKafkaConsumer.close();
            });
        return Observable.create(syncOnSubscribe)
            .subscribeOn(Schedulers.newThread())
            .doOnUnsubscribe(() -> LOGGER.info("consumer {} stopped due to unsubscribe", mantisKafkaConsumerId))
            .doOnError((t) -> {
                LOGGER.error("consumer {} stopped due to error", mantisKafkaConsumerId, t);
                consumerMetrics.incrementErrorCount();
            })
            .doOnTerminate(() -> LOGGER.info("consumer {} terminated", mantisKafkaConsumerId));
    }

    @Override
    public Observable<Observable<KafkaAckable>> call(Context context, Index index) {
        final int totalNumWorkers = index.getTotalNumWorkers();

        MantisKafkaSourceConfig mantisKafkaSourceConfig = new MantisKafkaSourceConfig(context);

        startAckProcessor();

        return Observable.create((Observable.OnSubscribe<Observable<KafkaAckable>>) child -> {

            final Observable<MantisKafkaConsumer<?>> consumers =
                createConsumers(context, mantisKafkaSourceConfig, totalNumWorkers);

            consumers.subscribe(consumer -> {
                final Observable<KafkaAckable> mantisKafkaAckableObs =
                    createBackPressuredConsumerObs(consumer, mantisKafkaSourceConfig);
                child.onNext(mantisKafkaAckableObs);
            });
        })
            .doOnUnsubscribe(() -> {
                LOGGER.info("unsubscribed");
                done.set(true);
            }).doOnSubscribe(() -> {
                LOGGER.info("subscribed");
                done.set(false);
            });

    }

    private void processAckNotification(final KafkaDataNotification notification) {
        final KafkaData kafkaData = notification.getValue();
        final TopicPartition topicPartition = new TopicPartition(kafkaData.getTopic(), kafkaData.getPartition());
        MantisKafkaConsumer<?> mantisKafkaConsumer = idToConsumerMap.get(kafkaData.getMantisKafkaConsumerId());

        if (mantisKafkaConsumer != null) {
            mantisKafkaConsumer.getPartitionStateManager().recordMessageAck(topicPartition, kafkaData.getOffset());
            if (!notification.isSuccess()) {
                // TODO provide a hook for the user to add handling for messages that could not be processed
                LOGGER.debug("Got negative acknowledgement {}", notification);
            }
            mantisKafkaConsumer.getConsumerMetrics().incrementProcessedCount();
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("got Ack for consumer id {} not in idToConsumerMap (topic {})", kafkaData.getMantisKafkaConsumerId(), kafkaData.getTopic());
            }
        }
    }

    private void startAckProcessor() {
        LOGGER.info("Acknowledgement processor started");
        ackSubject.subscribe((KafkaDataNotification notification) -> processAckNotification(notification));
    }

    @Override
    public List<ParameterDefinition<?>> getParameters() {
        final List<ParameterDefinition<?>> params = new ArrayList<>();
        params.add(new StringParameter()
                       .name(KafkaSourceParameters.TOPIC)
                       .description("Kafka topic to connect to")
                       .validator(Validators.notNullOrEmpty())
                       .required()
                       .build());
        params.add(new StringParameter()
                       .name(KafkaSourceParameters.KAFKA_VIP)
                       .description("vip address of source Kafka cluster")
                       .validator(Validators.notNullOrEmpty())
                       .required()
                       .build());
        // Optional parameters
        params.add(new StringParameter()
                       .name(KafkaSourceParameters.CHECKPOINT_STRATEGY)
                       .description("checkpoint strategy one of " + CheckpointStrategyOptions.values() + " (ensure enable.auto.commit param is set to false when enabling this)")
                       .defaultValue(CheckpointStrategyOptions.NONE)
                       .validator(Validators.alwaysPass())
                       .build());
        params.add(new IntParameter()
                       .name(KafkaSourceParameters.NUM_KAFKA_CONSUMER_PER_WORKER)
                       .description("No. of Kafka consumer instances per Mantis worker")
                       .validator(Validators.range(1, 16))
                       .defaultValue(DEFAULT_NUM_KAFKA_CONSUMER_PER_WORKER)
                       .build());
        params.add(new IntParameter()
                       .name(KafkaSourceParameters.MAX_BYTES_IN_PROCESSING)
                       .description("The maximum amount of data per-consumer awaiting acks to trigger an offsets commit. " +
                                        "These commits are in addition to any commits triggered by commitIntervalMs timer")
                       .defaultValue(DEFAULT_MAX_BYTES_IN_PROCESSING)
                       .validator(Validators.range(1, Integer.MAX_VALUE))
                       .build());
        params.add(new IntParameter()
                       .name(KafkaSourceParameters.CONSUMER_POLL_TIMEOUT_MS)
                       .validator(Validators.range(100, 10_000))
                       .defaultValue(250)
                       .build());
        params.add(new StringParameter()
                       .name(KafkaSourceParameters.PARSER_TYPE)
                       .validator(Validators.notNullOrEmpty())
                       .defaultValue(ParserType.SIMPLE_JSON.getPropName())
                       .build());
        params.add(new BooleanParameter()
                       .name(KafkaSourceParameters.PARSE_MSG_IN_SOURCE)
                       .validator(Validators.alwaysPass())
                       .defaultValue(DEFAULT_PARSE_MSG_IN_SOURCE)
                       .build());
        params.add(new BooleanParameter()
                       .name(KafkaSourceParameters.ENABLE_STATIC_PARTITION_ASSIGN)
                       .validator(Validators.alwaysPass())
                       .defaultValue(DEFAULT_ENABLE_STATIC_PARTITION_ASSIGN)
                       .description("Disable Kafka's default consumer group management and statically assign partitions to job workers. When enabling static partition assignments, disable auto-scaling and set the numPartitionsPerTopic job parameter")
                       .build());
        params.add(new StringParameter()
                       .name(KafkaSourceParameters.TOPIC_PARTITION_COUNTS)
                       .validator(Validators.alwaysPass())
                       .defaultValue("")
                       .description("Configures number of partitions on a kafka topic when static partition assignment is enabled. Format <topic1>:<numPartitions Topic1>,<topic2>:<numPartitions Topic2> Example: nf_errors_log:9,clevent:450")
                       .build());
        params.addAll(MantisKafkaConsumerConfig.getJobParameterDefinitions());
        return params;
    }

}
