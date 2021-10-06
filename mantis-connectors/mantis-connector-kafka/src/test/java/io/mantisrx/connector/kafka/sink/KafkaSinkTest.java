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

package io.mantisrx.connector.kafka.sink;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spectator.api.NoopRegistry;
import info.batey.kafka.unit.KafkaUnit;
import io.mantisrx.connector.kafka.ParameterTestUtils;
import io.mantisrx.connector.kafka.source.KafkaSourceTest;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.WorkerInfo;
import io.mantisrx.runtime.parameter.Parameters;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
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
public class KafkaSinkTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceTest.class);
    private static final KafkaUnit kafkaServer = new KafkaUnit(5000, 9092);
    private static final Random random = new Random(System.currentTimeMillis());
    private static final AtomicInteger topicNum = new AtomicInteger();

    @BeforeClass
    public static void startup() {
        kafkaServer.startup();
    }

    @AfterClass
    public static void shutdown() {
        kafkaServer.shutdown();
    }

    @Test
    public void testKafkaSink() throws InterruptedException {
        String testTopic = "testTopic" + topicNum.incrementAndGet();
        int numPartitions = 1;
        kafkaServer.createTopic(testTopic, numPartitions);
        int numMessages = 10;


        KafkaSink<String> kafkaSink = new KafkaSink<>(new NoopRegistry(), s -> s.getBytes());
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters(KafkaSinkJobParameters.TOPIC, testTopic);

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);
        when(context.getWorkerInfo()).then((Answer<WorkerInfo>) invocation -> new WorkerInfo("testJobName", "testJobName-1", 1, 0, 1, MantisJobDurationType.Perpetual, "1.1.1.1"));
        when(context.getJobId()).then((Answer<String>) invocation -> "testJobName-1");

        kafkaSink.call(context, mock(PortRequest.class), Observable.range(0, numMessages).map(x -> String.valueOf(x)));

        List<String> messages = kafkaServer.readAllMessages(testTopic);
        LOGGER.info("got {}", messages);
        assertEquals(numMessages, messages.size());
        for (int i = 0; i < numMessages; i++) {
            assertEquals(i, Integer.parseInt(messages.get(i)));
        }

        kafkaServer.deleteTopic(testTopic);
    }
}