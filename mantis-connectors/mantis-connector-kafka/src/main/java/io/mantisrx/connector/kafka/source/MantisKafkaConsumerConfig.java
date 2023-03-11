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

import io.mantisrx.common.MantisProperties;
import io.mantisrx.connector.kafka.KafkaSourceParameters;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.Parameters;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.shaded.com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility Class for handling Kafka ConsumerConfig defaults and Job parameter overrides
 */
public class MantisKafkaConsumerConfig extends ConsumerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantisKafkaConsumerConfig.class);

    public MantisKafkaConsumerConfig(Map<String, Object> props,
                                     Context context) {
        super(applyJobParamOverrides(context, props));
    }

    public MantisKafkaConsumerConfig(Context context) {
        this(defaultProps(), context);
    }

    public static final String DEFAULT_AUTO_OFFSET_RESET = "latest";
    public static final String DEFAULT_AUTO_COMMIT_ENABLED = "false";
    public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static final int DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 3000;
    public static final int DEFAULT_SESSION_TIMEOUT_MS = 10_000;
    public static final int DEFAULT_FETCH_MIN_BYTES = 1024;
    public static final int DEFAULT_FETCH_MAX_WAIT_MS = 100;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 40000;
    public static final int DEFAULT_CHECKPOINT_INTERVAL_MS = 5_000;
    public static final int DEFAULT_MAX_POLL_INTERVAL_MS = 300_000;
    public static final int DEFAULT_MAX_POLL_RECORDS = 500;
    public static final int DEFAULT_MAX_PARTITION_FETCH_BYTES = 10_000_000;
    public static final int DEFAULT_RECEIVE_BUFFER_BYTES = 32768;
    public static final int DEFAULT_SEND_BUFFER_BYTES = 131072;
    public static final Class<StringDeserializer> DEFAULT_KEY_DESERIALIZER = StringDeserializer.class;
    public static final Class<ByteArrayDeserializer> DEFAULT_VALUE_DESERIALIZER = ByteArrayDeserializer.class;

    public static Map<String, Object> defaultProps() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_AUTO_COMMIT_ENABLED);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_AUTO_COMMIT_INTERVAL_MS));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(DEFAULT_FETCH_MAX_WAIT_MS));
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(DEFAULT_FETCH_MIN_BYTES));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_HEARTBEAT_INTERVAL_MS));
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(DEFAULT_SESSION_TIMEOUT_MS));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(DEFAULT_MAX_PARTITION_FETCH_BYTES));
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, String.valueOf(DEFAULT_RECEIVE_BUFFER_BYTES));
        props.put(ConsumerConfig.SEND_BUFFER_CONFIG, String.valueOf(DEFAULT_SEND_BUFFER_BYTES));
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, JmxReporter.class.getName());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(DEFAULT_REQUEST_TIMEOUT_MS));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_RECORDS));
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_INTERVAL_MS));
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        return props;
    }

    /**
     * Get kafka consumer group ID to use by default.
     * @return default group ID to use for kafka consumer based on Mantis Job Id when running in cloud, else default to local consumer id
     */
    @VisibleForTesting
    static String getGroupId() {

        String jobId = MantisProperties.getProperty("JOB_ID");
        if (jobId != null && !jobId.isEmpty()) {
            LOGGER.info("default consumer groupId to {} if not overridden by job param", "mantis-kafka-source-" + jobId);
            return "mantis-kafka-source-" + jobId;
        }
        return "mantis-kafka-source-fallback-consumer-id";
    }

    private static Map<String, Object> applyJobParamOverrides(Context context, Map<String, Object> parsedValues) {
        final Parameters parameters = context.getParameters();
        if (!parsedValues.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            // set consumerGroupId if not already set
            final String consumerGroupId = (String) parameters.get(KafkaSourceParameters.PREFIX + ConsumerConfig.GROUP_ID_CONFIG, getGroupId());
            parsedValues.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        }

        for (String key : configNames()) {
            Object value = parameters.get(KafkaSourceParameters.PREFIX + key, null);
            if (value != null) {
                LOGGER.info("job param override for key {} -> {}", key, value);
                parsedValues.put(key, value);
            }
        }
        return parsedValues;
    }

    /**
     * Helper class to get all Kafka Consumer configs as Job Parameters to allow overriding Kafka consumer config settings at Job submit time.
     *
     * @return
     */
    public static List<ParameterDefinition<?>> getJobParameterDefinitions() {
        List<ParameterDefinition<?>> params = new ArrayList<>();
        Map<String, Object> defaultProps = defaultProps();
        for (String key : configNames()) {
            ParameterDefinition.Builder<String> builder = new StringParameter()
                .name(KafkaSourceParameters.PREFIX + key)
                .validator(Validators.alwaysPass())
                .description(KafkaSourceParameters.PREFIX + key);
            if (defaultProps.containsKey(key)) {
                Object value = defaultProps.get(key);
                if (value instanceof Class) {
                    builder = builder.defaultValue(((Class) value).getCanonicalName());
                } else {
                    builder = builder.defaultValue((String) value);
                }
            }
            params.add(builder.build());
        }
        return params;
    }

    public String getConsumerConfigStr() {
        return values().toString();
    }

    public Map<String, Object> getConsumerProperties() {
        return values().entrySet().stream()
            .filter(x -> x.getKey() != null && x.getValue() != null)
            .collect(Collectors.toMap(x -> x.getKey(),
                                      x -> (Object) x.getValue()));
    }
}
