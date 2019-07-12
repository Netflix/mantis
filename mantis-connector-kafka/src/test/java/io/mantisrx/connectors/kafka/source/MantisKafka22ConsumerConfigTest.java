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

import static io.mantisrx.connectors.kafka.source.ParameterTestUtils.createParameters;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;

import io.mantisrx.connectors.kafka.KafkaSourceParameters;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.Parameters;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.metrics.JmxReporter;
import org.junit.Test;
import org.mockito.stubbing.Answer;


public class MantisKafka22ConsumerConfigTest {

    @Test
    public void testDefaultConsumerConfig() {
        Context context = mock(Context.class);
        Parameters params = createParameters();

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);

        MantisKafka22ConsumerConfig mantisKafka22ConsumerConfig = new MantisKafka22ConsumerConfig(context);
        Map<String, Object> consumerProperties = mantisKafka22ConsumerConfig.getConsumerProperties();

        assertEquals(Boolean.valueOf(MantisKafka22ConsumerConfig.DEFAULT_AUTO_COMMIT_ENABLED), consumerProperties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_AUTO_COMMIT_INTERVAL_MS, consumerProperties.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_AUTO_OFFSET_RESET, consumerProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS, consumerProperties.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_FETCH_MIN_BYTES, consumerProperties.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_HEARTBEAT_INTERVAL_MS, consumerProperties.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_SESSION_TIMEOUT_MS, consumerProperties.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_KEY_DESERIALIZER, consumerProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_VALUE_DESERIALIZER, consumerProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES, consumerProperties.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_RECEIVE_BUFFER_BYTES, consumerProperties.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_SEND_BUFFER_BYTES, consumerProperties.get(ConsumerConfig.SEND_BUFFER_CONFIG));
        assertEquals(Arrays.asList(MantisKafka22ConsumerConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG), consumerProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(Arrays.asList(JmxReporter.class.getName()), consumerProperties.get(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG));
        assertEquals(Arrays.asList(RangeAssignor.class.getName()), consumerProperties.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.getGroupId(), consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_MAX_POLL_INTERVAL_MS, consumerProperties.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_MAX_POLL_RECORDS, consumerProperties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(MantisKafka22ConsumerConfig.DEFAULT_REQUEST_TIMEOUT_MS, consumerProperties.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @Test
    public void testJobParamOverrides() {
        Context context = mock(Context.class);
        String testTopic = "topic123";
        String testConsumerGroupId = "testKafkaConsumer-1";
        Parameters params = createParameters(KafkaSourceParameters.TOPIC, testTopic,
                                             ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                             ConsumerConfig.GROUP_ID_CONFIG, testConsumerGroupId);

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);

        MantisKafka22ConsumerConfig mantisKafka22ConsumerConfig = new MantisKafka22ConsumerConfig(context);
        Map<String, Object> consumerProperties = mantisKafka22ConsumerConfig.getConsumerProperties();
        // MantisKafka22ConsumerConfig only affects Kafka's ConsumerConfig defined properties
        assertFalse(ConsumerConfig.configNames().contains(KafkaSourceParameters.TOPIC));
        assertFalse(consumerProperties.containsKey(KafkaSourceParameters.TOPIC));

        assertEquals("earliest", consumerProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(testConsumerGroupId, consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG));
    }
}
