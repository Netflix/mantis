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


import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spectator.api.NoopRegistry;
import info.batey.kafka.unit.KafkaUnit;
import io.mantisrx.connector.kafka.KafkaAckable;
import io.mantisrx.connector.kafka.KafkaSourceParameters;
import io.mantisrx.connector.kafka.ParameterTestUtils;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.parameter.Parameters;
import io.mantisrx.runtime.source.Index;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

// Test ignored until kafka-unit dependency for kafka v2.2.+ is released after merging PR https://github.com/chbatey/kafka-unit/pull/69
@Ignore
public class KafkaSourceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceTest.class);
    private static final KafkaUnit kafkaServer = new KafkaUnit(5000, 9092);
    private static final Random random = new Random(System.currentTimeMillis());
    private static final AtomicInteger topicNum = new AtomicInteger(1);

    @BeforeClass
    public static void startup() {
        kafkaServer.startup();
    }

    @AfterClass
    public static void shutdown() {
        kafkaServer.shutdown();
    }

    @Test
    public void testKafkaSourceSingleConsumerReadsAllMessagesInOrderFromSinglePartition() throws InterruptedException {
        String testTopic = "testTopic" + topicNum.incrementAndGet();
        int numPartitions = 1;
        kafkaServer.createTopic(testTopic, numPartitions);
        int numMessages = 10;

        for (int i = 0; i < numMessages; i++) {
            ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic, "{\"messageNum\":"+i+"}");
            kafkaServer.sendMessages(keyedMessage);
        }


        KafkaSource kafkaSource = new KafkaSource(new NoopRegistry());
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters(KafkaSourceParameters.TOPIC, testTopic,
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.GROUP_ID_CONFIG, "testKafkaConsumer-" + random.nextInt());

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);
        when(context.getWorkerInfo()).then((Answer<WorkerInfo>) invocation -> new WorkerInfo("testJobName", "testJobName-1", 1, 0, 1, MantisJobDurationType.Perpetual, "1.1.1.1"));
        when(context.getJobId()).then((Answer<String>) invocation -> "testJobName-1");
        Index index = new Index(0, 10);
        Observable<Observable<KafkaAckable>> sourceObs = kafkaSource.call(context, index);
        final CountDownLatch latch = new CountDownLatch(numMessages);
        final AtomicInteger counter = new AtomicInteger(0);
        sourceObs
                .flatMap(kafkaAckableObs -> kafkaAckableObs)
                .map(kafkaAckable -> {
                    Optional<Map<String, Object>> parsedEvent = kafkaAckable.getKafkaData().getParsedEvent();
                    assertTrue(parsedEvent.isPresent());
                    assertEquals(counter.getAndIncrement(), parsedEvent.get().get("messageNum"));
                    LOGGER.info("got message on topic {} consumer Id {}", parsedEvent.get(), kafkaAckable.getKafkaData().getMantisKafkaConsumerId());
                    kafkaAckable.ack();
                    latch.countDown();
                    return parsedEvent;
                })
                .subscribe();
        assertTrue("timed out waiting to get all messages from Kafka", latch.await(10, TimeUnit.SECONDS));

        kafkaServer.deleteTopic(testTopic);
    }

    @Test
    public void testKafkaSourceSingleConsumerHandlesMessageParseFailures() throws InterruptedException {
        String testTopic = "testTopic" + topicNum.incrementAndGet();
        int numPartitions = 1;
        kafkaServer.createTopic(testTopic, numPartitions);
        int numMessages = 10;

        for (int i = 0; i < numMessages; i++) {
            ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic, "{\"messageNum\":"+i+"}");
            kafkaServer.sendMessages(keyedMessage);
            ProducerRecord<String, String> invalidJsonMessage = new ProducerRecord<>(testTopic, "{\"messageNum:"+i+"}");
            kafkaServer.sendMessages(invalidJsonMessage);
        }


        KafkaSource kafkaSource = new KafkaSource(new NoopRegistry());
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters(KafkaSourceParameters.TOPIC, testTopic,
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.GROUP_ID_CONFIG, "testKafkaConsumer-" + random.nextInt());

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);
        when(context.getWorkerInfo()).then((Answer<WorkerInfo>) invocation -> new WorkerInfo("testJobName", "testJobName-1", 1, 0, 1, MantisJobDurationType.Perpetual, "1.1.1.1"));
        when(context.getJobId()).then((Answer<String>) invocation -> "testJobName-1");
        Index index = new Index(0, 10);
        Observable<Observable<KafkaAckable>> sourceObs = kafkaSource.call(context, index);
        final CountDownLatch latch = new CountDownLatch(numMessages);
        final AtomicInteger counter = new AtomicInteger(0);
        sourceObs
            .flatMap(kafkaAckableObs -> kafkaAckableObs)
            .map(kafkaAckable -> {
                Optional<Map<String, Object>> parsedEvent = kafkaAckable.getKafkaData().getParsedEvent();
                assertTrue(parsedEvent.isPresent());
                assertEquals(counter.getAndIncrement(), parsedEvent.get().get("messageNum"));
                LOGGER.info("got message on topic {} consumer Id {}", parsedEvent.get(), kafkaAckable.getKafkaData().getMantisKafkaConsumerId());
                kafkaAckable.ack();
                latch.countDown();
                return parsedEvent;
            })
            .subscribe();
        assertTrue("timed out waiting to get all messages from Kafka", latch.await(30, TimeUnit.SECONDS));

        kafkaServer.deleteTopic(testTopic);
    }

    @Test
    public void testKafkaSourceMultipleConsumersReadsAllMessagesFromMultiplePartitions() throws InterruptedException {
        String testTopic = "testTopic" + topicNum.incrementAndGet();
        int numPartitions = 2;
        kafkaServer.createTopic(testTopic, numPartitions);
        int numMessages = 10;

        Set<Integer> outstandingMsgs = new ConcurrentSkipListSet<>();

        for (int i = 0; i < numMessages; i++) {
            ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic, "{\"messageNum\":"+i+"}");
            kafkaServer.sendMessages(keyedMessage);
            outstandingMsgs.add(i);
        }


        KafkaSource kafkaSource = new KafkaSource(new NoopRegistry());
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters(KafkaSourceParameters.NUM_KAFKA_CONSUMER_PER_WORKER, 2,
                                                                KafkaSourceParameters.TOPIC, testTopic,
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.GROUP_ID_CONFIG, "testKafkaConsumer-" + random.nextInt());

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);
        when(context.getWorkerInfo()).then((Answer<WorkerInfo>) invocation -> new WorkerInfo("testJobName", "testJobName-1", 1, 0, 1, MantisJobDurationType.Perpetual, "1.1.1.1"));
        when(context.getJobId()).then((Answer<String>) invocation -> "testJobName-1");
        Index index = new Index(0, 10);
        Observable<Observable<KafkaAckable>> sourceObs = kafkaSource.call(context, index);
        final CountDownLatch latch = new CountDownLatch(numMessages);
        final Map<Integer, Integer> lastMessageNumByConsumerId = new ConcurrentHashMap<>();

        sourceObs
            .flatMap(kafkaAckableObs -> kafkaAckableObs)
            .map(kafkaAckable -> {
                Optional<Map<String, Object>> parsedEvent = kafkaAckable.getKafkaData().getParsedEvent();

                assertTrue(parsedEvent.isPresent());
                Integer messageNum = (Integer)parsedEvent.get().get("messageNum");
                assertTrue(outstandingMsgs.contains(messageNum));
                outstandingMsgs.remove(messageNum);
                int mantisKafkaConsumerId = kafkaAckable.getKafkaData().getMantisKafkaConsumerId();
                lastMessageNumByConsumerId.putIfAbsent(mantisKafkaConsumerId, -1);
                // assert consumption of higher message numbers across consumer instances
                assertTrue(messageNum > lastMessageNumByConsumerId.get(mantisKafkaConsumerId));
                lastMessageNumByConsumerId.put(mantisKafkaConsumerId, messageNum);
                LOGGER.info("got message on topic {} consumer id {}", parsedEvent.get(), mantisKafkaConsumerId);
                kafkaAckable.ack();
                latch.countDown();
                return parsedEvent;
            })
            .doOnError(t -> {
                LOGGER.error("caught unexpected exception", t);
                fail("test failed due to unexpected error "+ t.getMessage());
            })
            .subscribe();
        assertTrue("timed out waiting to get all messages from Kafka", latch.await(10, TimeUnit.SECONDS));
        assertEquals(0, outstandingMsgs.size());
        assertTrue(lastMessageNumByConsumerId.keySet().size() == 2);
        lastMessageNumByConsumerId.keySet().forEach(consumerId -> {
            assertTrue(lastMessageNumByConsumerId.get(consumerId) >= 0);
        });
        kafkaServer.deleteTopic(testTopic);
    }

    @Test
    public void testKafkaSourceMultipleConsumersStaticPartitionAssignment() throws InterruptedException {
        String testTopic = "testTopic" + topicNum.incrementAndGet();
        int numConsumers = 3;
        int numPartitions = 3;
        kafkaServer.createTopic(testTopic, numPartitions);
        int numMessages = 10;

        Set<Integer> outstandingMsgs = new ConcurrentSkipListSet<>();

        for (int i = 0; i < numMessages; i++) {
            ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic, "{\"messageNum\":"+i+"}");
            kafkaServer.sendMessages(keyedMessage);
            outstandingMsgs.add(i);
        }


        KafkaSource kafkaSource = new KafkaSource(new NoopRegistry());
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters(KafkaSourceParameters.NUM_KAFKA_CONSUMER_PER_WORKER, numConsumers,
                                                                KafkaSourceParameters.TOPIC, testTopic,
                                                                KafkaSourceParameters.ENABLE_STATIC_PARTITION_ASSIGN, true,
                                                                KafkaSourceParameters.TOPIC_PARTITION_COUNTS, testTopic + ":" + numPartitions,
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.GROUP_ID_CONFIG, "testKafkaConsumer-" + random.nextInt());

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);
        when(context.getWorkerInfo()).then((Answer<WorkerInfo>) invocation -> new WorkerInfo("testJobName", "testJobName-1", 1, 0, 1, MantisJobDurationType.Perpetual, "1.1.1.1"));
        when(context.getJobId()).then((Answer<String>) invocation -> "testJobName-1");
        // Force all consumer instances to be created on same JVM by setting total number of workers for this job to 1
        int totalNumWorkerForJob = 1;
        Index index = new Index(0, totalNumWorkerForJob);
        Observable<Observable<KafkaAckable>> sourceObs = kafkaSource.call(context, index);
        final CountDownLatch latch = new CountDownLatch(numMessages);
        final Map<Integer, Integer> lastMessageNumByConsumerId = new ConcurrentHashMap<>();

        sourceObs
            .flatMap(kafkaAckableObs -> kafkaAckableObs)
            .map(kafkaAckable -> {
                Optional<Map<String, Object>> parsedEvent = kafkaAckable.getKafkaData().getParsedEvent();

                assertTrue(parsedEvent.isPresent());
                Integer messageNum = (Integer)parsedEvent.get().get("messageNum");
                assertTrue(outstandingMsgs.contains(messageNum));
                outstandingMsgs.remove(messageNum);
                int mantisKafkaConsumerId = kafkaAckable.getKafkaData().getMantisKafkaConsumerId();
                lastMessageNumByConsumerId.putIfAbsent(mantisKafkaConsumerId, -1);
                // assert consumption of higher message numbers across consumer instances
                assertTrue(messageNum > lastMessageNumByConsumerId.get(mantisKafkaConsumerId));
                lastMessageNumByConsumerId.put(mantisKafkaConsumerId, messageNum);
                LOGGER.info("got message on topic {} consumer id {}", parsedEvent.get(), mantisKafkaConsumerId);
                kafkaAckable.ack();
                latch.countDown();
                return parsedEvent;
            })
            .doOnError(t -> {
                LOGGER.error("caught unexpected exception", t);
                fail("test failed due to unexpected error "+ t.getMessage());
            })
            .subscribe();
        assertTrue("timed out waiting to get all messages from Kafka", latch.await(10, TimeUnit.SECONDS));
        assertEquals(0, outstandingMsgs.size());
        assertTrue(lastMessageNumByConsumerId.keySet().size() == numConsumers);
        lastMessageNumByConsumerId.keySet().forEach(consumerId -> {
            assertTrue(lastMessageNumByConsumerId.get(consumerId) >= 0);
        });
        kafkaServer.deleteTopic(testTopic);
    }
}
