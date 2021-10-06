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

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.Parameters;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MantisKafkaProducerConfig extends ProducerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantisKafkaProducerConfig.class);

    public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static final String DEFAULT_ACKS_CONFIG = "all";
    public static final int DEFAULT_RETRIES_CONFIG = 1;

    public MantisKafkaProducerConfig(Map<String, Object> props,
                                     Context context) {
        super(applyJobParamOverrides(context, props));
    }

    public MantisKafkaProducerConfig(Context context) {
        this(defaultProps(), context);
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(Map<String, Object> parsedValues) {
        return super.postProcessParsedConfig(parsedValues);
    }

    public static Map<String, Object> defaultProps() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, JmxReporter.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, DEFAULT_RETRIES_CONFIG);
        return props;
    }

    private static Map<String, Object> applyJobParamOverrides(Context context, Map<String, Object> parsedValues) {
        final Parameters parameters = context.getParameters();
        Map<String, Object> defaultProps = defaultProps();

        for (String key : configNames()) {
            Object value = parameters.get(KafkaSinkJobParameters.PREFIX + key, null);
            if (value != null) {
                LOGGER.info("job param override for key {} -> {}", key, value);
                parsedValues.put(key, value);
            }
        }

        final String bootstrapBrokers = (String) parameters.get(KafkaSinkJobParameters.PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, defaultProps.get(BOOTSTRAP_SERVERS_CONFIG));
        parsedValues.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

        final String clientId = (String) parameters.get(KafkaSinkJobParameters.PREFIX + ProducerConfig.CLIENT_ID_CONFIG, context.getJobId());
        parsedValues.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        return parsedValues;
    }

    public Map<String, Object> getProducerProperties() {
        return values().entrySet().stream()
            .filter(x -> x.getKey() != null && x.getValue() != null)
            .collect(Collectors.toMap(x -> x.getKey(),
                                      x -> (Object) x.getValue()));
    }

    /**
     * Helper class to get all Kafka Producer configs as Job Parameters to allow overriding Kafka producer config settings at Job submit time.
     *
     * @return
     */
    public static List<ParameterDefinition<?>> getJobParameterDefinitions() {
        List<ParameterDefinition<?>> params = new ArrayList<>();
        Map<String, Object> defaultProps = defaultProps();
        for (String key : configNames()) {
            ParameterDefinition.Builder<String> builder = new StringParameter()
                .name(KafkaSinkJobParameters.PREFIX + key)
                .validator(Validators.alwaysPass())
                .description(KafkaSinkJobParameters.PREFIX + key);
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
}
