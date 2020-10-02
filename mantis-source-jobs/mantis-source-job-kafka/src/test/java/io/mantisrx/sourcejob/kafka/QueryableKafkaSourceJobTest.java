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

package io.mantisrx.sourcejob.kafka;


import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import info.batey.kafka.unit.KafkaUnit;
import io.mantisrx.connector.kafka.KafkaSourceParameters;
import io.mantisrx.connector.kafka.source.serde.ParserType;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.parameter.Parameter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import rx.schedulers.Schedulers;


// Test ignored until kafka-unit dependency for kafka v2.2.+ is released after merging PR https://github.com/chbatey/kafka-unit/pull/69
@Ignore
public class QueryableKafkaSourceJobTest {
    private static final KafkaUnit kafkaServer = new KafkaUnit(5000, 9092);
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
    @Ignore
    public void testKafkaSourceSingleConsumerReadsAllMessagesInOrderFromSinglePartition() throws InterruptedException {
        // TODO modify Local executor to allow for passing in a static PortSelector for the sink port for integration testing and
        //  countdown latch on receiving message from sink port
        String testTopic = "testTopic" + topicNum.incrementAndGet();
        int numPartitions = 1;
        kafkaServer.createTopic(testTopic, numPartitions);
        int numMessages = 1;

        AtomicInteger counter = new AtomicInteger();
        Schedulers.newThread().createWorker().schedulePeriodically(() -> {
            ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic, "{\"messageNum\":" + counter.incrementAndGet() + "}");
            kafkaServer.sendMessages(keyedMessage);
        }, 1, 1, TimeUnit.SECONDS);

        LocalJobExecutorNetworked.execute(new QueryableKafkaSourceJob().getJobInstance(),
                                          new Parameter(KafkaSourceParameters.TOPIC, testTopic),
                                          new Parameter(KafkaSourceParameters.NUM_KAFKA_CONSUMER_PER_WORKER, "1"),
                                          new Parameter(KafkaSourceParameters.PARSER_TYPE, ParserType.SIMPLE_JSON.getPropName()),
                                          new Parameter(KafkaSourceParameters.PARSE_MSG_IN_SOURCE, "true"),
                                          new Parameter(KafkaSourceParameters.PREFIX + ConsumerConfig.GROUP_ID_CONFIG, "QueryableKafkaSourceLocal"),
                                          new Parameter(KafkaSourceParameters.PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
                                          new Parameter(KafkaSourceParameters.PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

        final CountDownLatch latch = new CountDownLatch(numMessages);
        assertTrue("timed out waiting to get all messages from Kafka", latch.await(10, TimeUnit.MINUTES));

        kafkaServer.deleteTopic(testTopic);
    }
}
