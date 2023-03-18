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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.mantisrx.connector.kafka.KafkaSourceParameters;
import io.mantisrx.connector.kafka.ParameterTestUtils;
import io.mantisrx.connector.kafka.source.checkpoint.strategy.CheckpointStrategyOptions;
import io.mantisrx.connector.kafka.source.serde.ParserType;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.Parameters;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;


public class MantisKafkaSourceConfigTest {

    @Test
    public void testDefaultConsumerConfig() {
        Context context = mock(Context.class);
        Parameters params = ParameterTestUtils.createParameters(KafkaSourceParameters.TOPIC, "testTopic");

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);

        MantisKafkaSourceConfig mantisKafkaSourceConfig = new MantisKafkaSourceConfig(context);

        assertEquals(MantisKafkaSourceConfig.DEFAULT_CONSUMER_POLL_TIMEOUT_MS, mantisKafkaSourceConfig.getConsumerPollTimeoutMs());
        assertEquals(CheckpointStrategyOptions.NONE, mantisKafkaSourceConfig.getCheckpointStrategy());
        assertEquals(MantisKafkaConsumerConfig.DEFAULT_CHECKPOINT_INTERVAL_MS, mantisKafkaSourceConfig.getCheckpointIntervalMs());
        assertEquals(MantisKafkaSourceConfig.DEFAULT_MAX_BYTES_IN_PROCESSING, mantisKafkaSourceConfig.getMaxBytesInProcessing());
        assertEquals(ParserType.SIMPLE_JSON.getPropName(), mantisKafkaSourceConfig.getMessageParserType());
        assertEquals(MantisKafkaSourceConfig.DEFAULT_NUM_KAFKA_CONSUMER_PER_WORKER, mantisKafkaSourceConfig.getNumConsumerInstances());
        assertEquals(MantisKafkaSourceConfig.DEFAULT_PARSE_MSG_IN_SOURCE, mantisKafkaSourceConfig.getParseMessageInSource());
        assertEquals(MantisKafkaSourceConfig.DEFAULT_RETRY_CHECKPOINT_CHECK_DELAY_MS, mantisKafkaSourceConfig.getRetryCheckpointCheckDelayMs());
        assertEquals(MantisKafkaSourceConfig.DEFAULT_ENABLE_STATIC_PARTITION_ASSIGN, mantisKafkaSourceConfig.getStaticPartitionAssignmentEnabled());
        assertEquals(Optional.empty(), mantisKafkaSourceConfig.getTopicPartitionCounts());
        assertEquals(Arrays.asList("testTopic"), mantisKafkaSourceConfig.getTopics());

    }

    @Test
    public void testJobParamOverrides() {
        Context context = mock(Context.class);
        String testTopic = "topic123";
        int checkpointIntervalOverride = 100;
        boolean staticPartitionAssignEnableOverride = true;

        Parameters params = ParameterTestUtils.createParameters(KafkaSourceParameters.TOPIC, testTopic,
                                                                KafkaSourceParameters.CHECKPOINT_INTERVAL_MS, checkpointIntervalOverride,
                                                                KafkaSourceParameters.ENABLE_STATIC_PARTITION_ASSIGN, staticPartitionAssignEnableOverride,
                                                                KafkaSourceParameters.TOPIC_PARTITION_COUNTS, testTopic+":1024");

        when(context.getParameters()).then((Answer<Parameters>) invocation -> params);

        MantisKafkaSourceConfig mantisKafkaSourceConfig = new MantisKafkaSourceConfig(context);
        assertEquals(checkpointIntervalOverride, mantisKafkaSourceConfig.getCheckpointIntervalMs());
        assertEquals(staticPartitionAssignEnableOverride, mantisKafkaSourceConfig.getStaticPartitionAssignmentEnabled());
        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put(testTopic, 1024);
        assertEquals(Optional.ofNullable(topicPartitionCounts), mantisKafkaSourceConfig.getTopicPartitionCounts());

    }
}
