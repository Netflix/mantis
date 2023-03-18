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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.mantisrx.connector.kafka.KafkaSourceParameters;
import io.mantisrx.connector.kafka.ParameterTestUtils;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.Parameters;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.metrics.JmxReporter;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;


public class MantisKafkaConsumerConfigTest {

    @Test
    public void testDefaultConsumerConfig() {
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters();

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);

        MantisKafkaConsumerConfig mantisKafkaConsumerConfig = new MantisKafkaConsumerConfig(context);
        Map<String, Object> consumerProperties = mantisKafkaConsumerConfig.getConsumerProperties();

        assertEquals(Boolean.valueOf(MantisKafkaConsumerConfig.DEFAULT_AUTO_COMMIT_ENABLED), consumerProperties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_AUTO_COMMIT_INTERVAL_MS, consumerProperties.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_AUTO_OFFSET_RESET, consumerProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS, consumerProperties.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_FETCH_MIN_BYTES, consumerProperties.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_HEARTBEAT_INTERVAL_MS, consumerProperties.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_SESSION_TIMEOUT_MS, consumerProperties.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_KEY_DESERIALIZER, consumerProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_VALUE_DESERIALIZER, consumerProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES, consumerProperties.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_RECEIVE_BUFFER_BYTES, consumerProperties.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_SEND_BUFFER_BYTES, consumerProperties.get(ConsumerConfig.SEND_BUFFER_CONFIG));
        assertEquals(Arrays.asList(MantisKafkaConsumerConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG), consumerProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(Arrays.asList(JmxReporter.class.getName()), consumerProperties.get(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG));
        assertEquals(Arrays.asList(RangeAssignor.class.getName()), consumerProperties.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.getGroupId(), consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_MAX_POLL_INTERVAL_MS, consumerProperties.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_MAX_POLL_RECORDS, consumerProperties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_REQUEST_TIMEOUT_MS, consumerProperties.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));
    }

    @Test
    public void testJobParamOverrides() {
        Context context = mock(Context.class);
        String testTopic = "topic123";
        String testConsumerGroupId = "testKafkaConsumer-1";
        Parameters params = ParameterTestUtils.createParameters(KafkaSourceParameters.TOPIC, testTopic,
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.GROUP_ID_CONFIG, testConsumerGroupId,
                                                                KafkaSourceParameters.PREFIX + ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 500);

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);

        MantisKafkaConsumerConfig mantisKafkaConsumerConfig = new MantisKafkaConsumerConfig(context);
        Map<String, Object> consumerProperties = mantisKafkaConsumerConfig.getConsumerProperties();
        // MantisKafkaConsumerConfig only affects Kafka's ConsumerConfig defined properties
        assertFalse(ConsumerConfig.configNames().contains(KafkaSourceParameters.TOPIC));
        assertFalse(consumerProperties.containsKey(KafkaSourceParameters.TOPIC));

        assertEquals("earliest", consumerProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(testConsumerGroupId, consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(500, consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }
}
