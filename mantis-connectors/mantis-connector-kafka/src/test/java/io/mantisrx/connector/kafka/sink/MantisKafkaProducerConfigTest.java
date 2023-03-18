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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.mantisrx.connector.kafka.ParameterTestUtils;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.Parameters;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;


public class MantisKafkaProducerConfigTest {

    @Test
    public void testDefaultKafkaProducerConfig() {
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters();

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);

        MantisKafkaProducerConfig mantisKafkaProducerConfig= new MantisKafkaProducerConfig(context);
        Map<String, Object> producerProperties = mantisKafkaProducerConfig.getProducerProperties();

        assertEquals(ByteArraySerializer.class, producerProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArraySerializer.class, producerProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals(Arrays.asList(MantisKafkaProducerConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG), producerProperties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(Arrays.asList(JmxReporter.class.getName()), producerProperties.get(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG));
    }

    @Test
    public void testJobParamOverrides() {
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters(KafkaSinkJobParameters.PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);

        MantisKafkaProducerConfig mantisKafkaProducerConfig= new MantisKafkaProducerConfig(context);
        Map<String, Object> producerProperties = mantisKafkaProducerConfig.getProducerProperties();

        assertEquals(StringSerializer.class, producerProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArraySerializer.class, producerProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }
}
