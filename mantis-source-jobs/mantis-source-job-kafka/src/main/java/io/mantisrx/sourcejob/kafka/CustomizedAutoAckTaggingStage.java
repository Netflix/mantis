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

import static io.mantisrx.common.SystemParameters.STAGE_CONCURRENCY;

import io.mantisrx.connector.kafka.KafkaAckable;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import io.mantisrx.sourcejob.kafka.core.TaggedData;
import io.mantisrx.sourcejob.kafka.core.utils.JsonUtility;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;


public class CustomizedAutoAckTaggingStage extends AutoAckTaggingStage {

    private static Logger logger = LoggerFactory.getLogger(CustomizedAutoAckTaggingStage.class);
    private String jobName;

    private String timestampField = "_ts_";
    private AtomicLong latestTimeStamp = new AtomicLong();

    private boolean isFlattenFields = false;
    private List<String> fieldsToFlatten = new ArrayList<>();

    private Func1<Map<String, Object>, Map<String, Object>> preMapperFunc = t -> t;

    @Override
    public void init(Context context) {
        super.init(context);
        jobName = context.getWorkerInfo().getJobName();

        String timeStampFieldParam = System.getenv("JOB_PARAM_timeStampField");
        if (timeStampFieldParam != null && !timeStampFieldParam.isEmpty()) {
            this.timestampField = timeStampFieldParam;
        }

        String flattenFieldsStr = System.getenv("JOB_PARAM_fieldsToFlatten");
        if (flattenFieldsStr != null && !flattenFieldsStr.isEmpty() && !flattenFieldsStr.equals("NONE")) {

            String[] fields = flattenFieldsStr.split(",");
            if (fields.length > 0) {
                isFlattenFields = true;
                for (String field : fields) {
                    fieldsToFlatten.add(field.trim());
                }
                logger.info("Field flattening enabled for fields {}", fieldsToFlatten);
            }

        }

    }

    private void flattenFields(Map<String, Object> rawData) {
        for (String field : fieldsToFlatten) {
            flattenField(rawData, field);
        }
    }

    private void flattenField(Map<String, Object> rawData, String fieldName) {
        String dataJson = (String) rawData.get(fieldName);
        try {
            Map<String, Object> geoDataMap = JsonUtility.jsonToMap(dataJson);
            Iterator<Entry<String, Object>> it = geoDataMap.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Object> e = it.next();
                String key = e.getKey();
                Object val = e.getValue();
                if (key != null && val != null) {
                    rawData.put(fieldName + "." + key, val);
                }
            }
        } catch (Exception e) {
            logger.warn("Error flattening field " + fieldName + " error -> " + e.getMessage());
        }
    }

    @Override
    protected Map<String, Object> applyPreMapping(final Context context, final Map<String, Object> rawData) {
        if (rawData == null) {
            throw new RuntimeException("rawData is null");
        }
        long now = System.currentTimeMillis();
        if (rawData.containsKey(timestampField)) {
            long ts = (Long) rawData.get(timestampField);
            long latestTsYet = latestTimeStamp.get();
            if (ts > latestTsYet) {
                latestTimeStamp.compareAndSet(latestTsYet, ts);
            }
            // TODO DynamicGauge.set("manits.source.timelag", (now - latestTimeStamp.get()));
        }
        try {
            preMapperFunc.call(rawData);
        } catch (Exception e) {
            // TODO DynamicCounter.increment("mantis.source.premapping.failed", "mantisJobName", jobName);
            logger.warn("Exception applying premapping function " + e.getMessage());
        }
        final Map<String, Object> modifiedData = new HashMap<>(rawData);

        modifiedData.put(MANTIS_META_SOURCE_NAME, jobName);
        modifiedData.put(MANTIS_META_SOURCE_TIMESTAMP, now);
        if (isFlattenFields) {
            flattenFields(modifiedData);
        }

        return modifiedData;
    }

    public static ScalarToScalar.Config<KafkaAckable, TaggedData> config() {
        ScalarToScalar.Config<KafkaAckable, TaggedData> config = new ScalarToScalar.Config<KafkaAckable, TaggedData>()
            .concurrentInput()
            .codec(AutoAckTaggingStage.taggedDataCodec())
            .withParameters(getParameters());

        String jobParamPrefix = "JOB_PARAM_";
        String stageConcurrencyParam = jobParamPrefix + STAGE_CONCURRENCY;
        String concurrency = System.getenv(stageConcurrencyParam);
        if (concurrency != null && !concurrency.isEmpty()) {
            logger.info("Job param: " + stageConcurrencyParam + " value: " + concurrency);
            try {
                config = config.concurrentInput(Integer.parseInt(concurrency));
            } catch (NumberFormatException ignored) {

            }
        }

        return config;
    }

    static List<ParameterDefinition<?>> getParameters() {

        List<ParameterDefinition<?>> params = Lists.newArrayList();
        // queryable source parameters
        params.add(new StringParameter()
                       .name("fieldsToFlatten")
                       .description("comma separated list of fields to flatten")
                       .validator(Validators.notNullOrEmpty())
                       .defaultValue("NONE")
                       .build());
        params.add(new StringParameter()
                       .name("timeStampField")
                       .description("the timestamp field in the event. used to calculate lag")
                       .validator(Validators.notNullOrEmpty())
                       .defaultValue("_ts_")
                       .build());
        params.add(new IntParameter()
                       .name(STAGE_CONCURRENCY)
                       .description("Parameter to control number of computation workers to use for stage processing")
                       .defaultValue(1)
                       .validator(Validators.range(1, 8))
                       .build());

        return params;
    }
}
