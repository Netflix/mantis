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

package io.mantisrx.runtime.parameter;

import static io.mantisrx.common.SystemParameters.*;

import com.mantisrx.common.utils.MantisSSEConstants;
import io.mantisrx.common.compression.CompressionUtils;
import io.mantisrx.runtime.parameter.type.BooleanParameter;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validation;
import io.mantisrx.runtime.parameter.validator.Validators;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import rx.functions.Func1;


@Slf4j
public class ParameterUtils {

    static final Map<String, ParameterDefinition<?>> systemParams = new ConcurrentHashMap<>();

    static {
        ParameterDefinition<Integer> keyBuffer = new IntParameter()
                .name("mantis.w2w.toKeyBuffer")
                .validator(Validators.range(1, 100000))
                .defaultValue(50000)
                .description("per connection buffer from Scalar To Key stage")
                .build();
        systemParams.put(keyBuffer.getName(), keyBuffer);

        ParameterDefinition<Boolean> useSPSC4sse = new BooleanParameter()
                .name("mantis.sse.spsc")
                .description("Whether to use spsc or blocking queue for SSE")
                .defaultValue(false)
                .build();

        systemParams.put(useSPSC4sse.getName(), useSPSC4sse);

        ParameterDefinition<Boolean> useSPSC4w2w = new BooleanParameter()
                .name("mantis.w2w.spsc")
                .description("Whether to use spsc or blocking queue")
                .defaultValue(false)
                .build();

        systemParams.put(useSPSC4w2w.getName(), useSPSC4w2w);


        ParameterDefinition<Boolean> singleNettyThread = new BooleanParameter()
                .name("mantis.netty.useSingleThread")
                .description("use single netty thread")
                .defaultValue(false)
                .build();

        systemParams.put(singleNettyThread.getName(), singleNettyThread);


        //mantis.w2w.toKeyMaxChunkSize 1000
        ParameterDefinition<Integer> w2wtoKeyMaxChunkSize = new IntParameter()
                .name("mantis.w2w.toKeyMaxChunkSize")
                .validator(Validators.range(1, 100000))
                .defaultValue(1000)
                .description("batch size for bytes drained from Scalar To Key stage")
                .build();
        systemParams.put(w2wtoKeyMaxChunkSize.getName(), w2wtoKeyMaxChunkSize);

        //mantis.w2w.toKeyThreads 1
        ParameterDefinition<Integer> w2wtoKeyThreads = new IntParameter()
                .name("mantis.w2w.toKeyThreads")
                .validator(Validators.range(1, 8))
                .description("number of drainer threads on the ScalarToKey stage")
                .defaultValue(1)
                .build();
        systemParams.put(w2wtoKeyThreads.getName(), w2wtoKeyThreads);


        // mantis.sse.bufferCapacity 25000
        ParameterDefinition<Integer> sseBuffer = new IntParameter()
                .name("mantis.sse.bufferCapacity")
                .validator(Validators.range(1, 100000))
                .description("buffer on SSE per connection")
                .defaultValue(25000)
                .build();
        systemParams.put(sseBuffer.getName(), sseBuffer);

        //mantis.sse.maxChunkSize 1000

        ParameterDefinition<Integer> sseChunkSize = new IntParameter()
                .name("mantis.sse.maxChunkSize")
                .validator(Validators.range(1, 100000))
                .description("SSE chunk size")
                .defaultValue(1000)
                .build();
        systemParams.put(sseChunkSize.getName(), sseChunkSize);

        // mantis.sse.maxReadTimeMSec", "250"

        ParameterDefinition<Integer> sseMaxReadTime = new IntParameter()
                .name("mantis.sse.maxReadTimeMSec")
                .validator(Validators.range(1, 100000))
                .description("interval at which buffer is drained to write to SSE")
                .defaultValue(250)
                .build();
        systemParams.put(sseMaxReadTime.getName(), sseMaxReadTime);

        //mantis.sse.numConsumerThreads 1

        ParameterDefinition<Integer> sse_numConsumerThreads = new IntParameter()
                .name("mantis.sse.numConsumerThreads")
                .validator(Validators.range(1, 64))
                .description("number of consumer threads draining the queue to write to SSE")
                .defaultValue(1)
                .build();
        systemParams.put(sse_numConsumerThreads.getName(), sse_numConsumerThreads);

        // mantis.sse.maxNotWritableTimeSec", "-1"

        ParameterDefinition<Integer> maxNotWritableTimeSec = new IntParameter()
            .name("mantis.sse.maxNotWritableTimeSec")
            .validator(Validators.range(-1, 100000))
            .description("maximum time the SSE connection can remain not writable before we proactively terminated it on server side. <= 0 means unlimited.")
            .defaultValue(-1)
            .build();
        systemParams.put(maxNotWritableTimeSec.getName(), maxNotWritableTimeSec);

        // mantis.jobmaster.autoscale.metric
        ParameterDefinition<String> jobMasterAutoScaleMetric = new StringParameter()
                .name(JOB_MASTER_AUTOSCALE_METRIC_SYSTEM_PARAM)
                .validator(Validators.alwaysPass())
                .description("Custom autoscale metric for Job Master to use with UserDefined Scaling Strategy. Format: <metricGroup>::<metricName>::<algo> where metricGroup and metricName should exactly match the metric published via Mantis MetricsRegistry and algo = MAX/AVERAGE")
                .defaultValue("")
                .build();
        systemParams.put(jobMasterAutoScaleMetric.getName(), jobMasterAutoScaleMetric);

        // mantis.jobmaster.clutch.config
        ParameterDefinition<String> jobMasterAutoScaleConfig = new StringParameter()
                .name(JOB_MASTER_CLUTCH_SYSTEM_PARAM)
                .validator(Validators.alwaysPass())
                .description("Configuration for the clutch autoscaler.")
                .defaultValue("")
                .build();
        systemParams.put(jobMasterAutoScaleConfig.getName(), jobMasterAutoScaleConfig);

        // mantis.jobmaster.clutch.experimental.enabled
        ParameterDefinition<Boolean> clutchExperimentalEnabled = new BooleanParameter()
                .name(JOB_MASTER_CLUTCH_EXPERIMENTAL_PARAM)
                .validator(Validators.alwaysPass())
                .description("Enables the experimental version of the Clutch autoscaler. Note this is different from the Clutch used in production today.")
                .defaultValue(false)
                .build();
        systemParams.put(clutchExperimentalEnabled.getName(), clutchExperimentalEnabled);

        ParameterDefinition<Integer> stageConcurrency = new IntParameter()
                .name(STAGE_CONCURRENCY)
                .validator(Validators.range(-1, 16))
                .defaultValue(-1)
                .description("Number of cores to use for stage processing")
                .build();
        systemParams.put(stageConcurrency.getName(), stageConcurrency);

        ParameterDefinition<Boolean> sseBinary = new BooleanParameter()
                .name(MantisSSEConstants.MANTIS_ENABLE_COMPRESSION)
                .validator(Validators.alwaysPass())
                .defaultValue(false)
                .description("Enables binary compression of SSE data")
                .build();
        systemParams.put(sseBinary.getName(), sseBinary);

        ParameterDefinition<String> compressionDelimiter = new StringParameter()
                .name(MantisSSEConstants.MANTIS_COMPRESSION_DELIMITER)
                .validator(Validators.alwaysPass())
                .defaultValue(CompressionUtils.MANTIS_SSE_DELIMITER)
                .description("Delimiter for separating SSE data before compression")
                .build();
        systemParams.put(compressionDelimiter.getName(), compressionDelimiter);

        ParameterDefinition<Boolean> autoscaleSourceJobMetricEnabled = new BooleanParameter()
                .name(JOB_MASTER_AUTOSCALE_SOURCEJOB_METRIC_PARAM)
                .validator(Validators.alwaysPass())
                .defaultValue(false)
                .description("Enable source job drop metrics to be used for autoscaling the 1st stage")
                .build();
        systemParams.put(autoscaleSourceJobMetricEnabled.getName(), autoscaleSourceJobMetricEnabled);

        ParameterDefinition<String> autoscaleSourceJobTarget = new StringParameter()
                .name(JOB_MASTER_AUTOSCALE_SOURCEJOB_TARGET_PARAM)
                .validator(Validators.alwaysPass())
                .defaultValue("{}")
                .description("Json config to specify source job targets for autoscale metrics. This param is not needed if the 'target' param is already present. Example: {\"targets\": [{\"sourceJobName\":<jobName>, \"clientId\":<clientId>}]}")
                .build();
        systemParams.put(autoscaleSourceJobTarget.getName(), autoscaleSourceJobTarget);

        ParameterDefinition<String> autoscaleSourceJobDropMetricPattern = new StringParameter()
                .name(JOB_MASTER_AUTOSCALE_SOURCEJOB_DROP_METRIC_PATTERNS_PARAM)
                .validator(Validators.alwaysPass())
                .defaultValue("")
                .description("Additional metrics pattern for source job drops. Comma separated list, supports dynamic client ID by using '_CLIENT_ID_' as a token. " +
                        "Each metric should be expressed in the same format as '" + JOB_MASTER_AUTOSCALE_METRIC_SYSTEM_PARAM + "'. " +
                        "Example: PushServerSse:clientId=_CLIENT_ID_:*::droppedCounter::MAX,ServerSentEventRequestHandler:clientId=_CLIENT_ID_:*::droppedCounter::MAX")
                .build();
        systemParams.put(autoscaleSourceJobDropMetricPattern.getName(), autoscaleSourceJobDropMetricPattern);

        ParameterDefinition<Integer> workerHeartbeatInterval = new IntParameter()
                .name(JOB_WORKER_HEARTBEAT_INTERVAL_SECS)
                .validator(Validators.alwaysPass())
                .defaultValue(0)
                .description("Configures heartbeat interval (in seconds) for job workers. This is useful to configure worker restart logic.")
                .build();
        systemParams.put(workerHeartbeatInterval.getName(), workerHeartbeatInterval);

        ParameterDefinition<Integer> workerTimeout = new IntParameter()
            .name(JOB_WORKER_TIMEOUT_SECS)
            .validator(Validators.alwaysPass())
            .defaultValue(0)
            .description("Configures timeout interval (in seconds) for job workers. There is some grace period and retries " +
                "built in to allow for network delays and/or miss a few worker heartbeats before being killed.")
            .build();
        systemParams.put(workerTimeout.getName(), workerTimeout);
    }

    private ParameterUtils() {

    }


    public static Parameters createContextParameters(
            Map<String, ParameterDefinition<?>> parameterDefinitions,
            Parameter... parameters) {
        // index parameters by name
        Map<String, Parameter> indexed = new HashMap<>();
        parameterDefinitions.putAll(systemParams);
        for (Parameter parameter : parameters) {
            indexed.put(parameter.getName(), parameter);
        }
        return new Parameters(checkThenCreateState(parameterDefinitions,
                indexed), getRequiredParameters(parameterDefinitions),
                getParameterDefinitions(parameterDefinitions));
    }

    public static Parameters createContextParameters(
            Map<String, ParameterDefinition<?>> parameterDefinitions,
            List<Parameter> parameters) {
        parameterDefinitions.putAll(systemParams);
        Parameter[] array = parameters.toArray(new Parameter[parameters.size()]);
        return createContextParameters(parameterDefinitions, array);
    }

    @SuppressWarnings( {"unchecked", "rawtypes"})
    private static void validationCheck(Func1 validator, Object value,
                                        String name) throws IllegalArgumentException {
        if (validator == null) {
            throw new IllegalArgumentException("Validator for parameter definition: " + name + " is null");
        }
        Validation validatorOutcome;
        try {
            validatorOutcome = (Validation) validator.call(value);
        } catch (Throwable t) {
            throw new IllegalArgumentException("Parameter: " + name + " with value: "
                    + value + " failed validator: "
                    + t.getMessage(), t);
        }
        if (validatorOutcome.isFailedValidation()) {
            throw new IllegalArgumentException("Parameter: " + name + " with value: "
                    + value + " failed validator: "
                    + validatorOutcome.getFailedValidationReason());
        }
    }

    @SuppressWarnings( {"rawtypes"})
    public static <T> Map<String, Object> checkThenCreateState(
            Map<String, ParameterDefinition<?>> parameterDefinitions,
            Map<String, Parameter> parameters) throws IllegalArgumentException {
        Map<String, Object> parameterState = new HashMap<>();

        // check all required parameters are present
        for (ParameterDefinition<?> definition : parameterDefinitions.values()) {
            if (definition.isRequired()) {
                // check if required is present in parameters
                // and there is not a default value
                if (!parameters.containsKey(definition.getName()) &&
                        definition.getDefaultValue() == null) {
                    throw new IllegalArgumentException("Missing required parameter: " + definition.getName() + ", check job parameter definitions.");
                }
            }
            // if default value, check validation
            if (definition.getDefaultValue() != null) {
                validationCheck(definition.getValidator().getValidator(), definition.getDefaultValue(),
                        "[default value] " + definition.getName());
                // add default value
                parameterState.put(definition.getName(), definition.getDefaultValue());
            }
        }

        // run all validators
        for (Parameter parameter : parameters.values()) {
            String name = parameter.getName();
            Object value;
            ParameterDefinition<?> definition;

            definition = parameterDefinitions.get(name);

            if (definition == null) {
                if (name.equals("MANTIS_WORKER_JVM_OPTS") || name.startsWith(MANTIS_WORKER_JVM_OPTS_STAGE_PREFIX)) {
                    log.warn("Ignoring invalid parameter definitions with name: {}, will skip parameter", name);
                    continue;
                } else if (name.startsWith("mantis.") || name.startsWith("MANTIS")) {
                    log.info("mantis runtime parameter {} used, looking up definition >>>", name);
                    definition = systemParams.get(name);
                } else {
                    log.warn("No parameter definition for parameter with name: {}, will skip parameter", name);
                    continue;
                }
            }
            Func1 validator = definition.getValidator().getValidator();
            value = definition.getDecoder().decode(parameter.getValue());
            validationCheck(validator, value, name);

            parameterState.put(name, value);
        }
        return parameterState;
    }

    public static Set<String> getRequiredParameters(Map<String, ParameterDefinition<?>> parameterDefinitions) {
        Set<String> requiredParameters = new HashSet<>();
        for (ParameterDefinition<?> definition : parameterDefinitions.values()) {
            if (definition.isRequired()) {
                requiredParameters.add(definition.getName());
            }
        }
        return requiredParameters;
    }

    public static Set<String> getParameterDefinitions(Map<String, ParameterDefinition<?>> parameterDefinitions) {
        Set<String> parameters = new HashSet<>();
        for (ParameterDefinition<?> definition : parameterDefinitions.values()) {
            parameters.add(definition.getName());
        }
        return parameters;
    }

    public static Map<String, ParameterDefinition<?>> getSystemParameters() {
        return Collections.unmodifiableMap(systemParams);
    }
}
