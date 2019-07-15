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

package io.mantisrx.sourcejobs.publish.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.mantisrx.common.utils.MantisSourceJobConstants;
import io.mantisrx.common.codec.Codecs;
import io.mantisrx.publish.proto.MantisEventEnvelope;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import org.apache.log4j.Logger;
import rx.Observable;


public class EchoStage implements ScalarComputation<String, String> {

    private static final Logger LOGGER = Logger.getLogger(EchoStage.class);

    private String clusterName;

    private int bufferDuration = 100;
    private String sourceNamePrefix;
    private ObjectReader mantisEventEnvelopeReader;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void init(Context context) {
        clusterName = context.getWorkerInfo().getJobClusterName();
        bufferDuration = (int) context.getParameters().get(MantisSourceJobConstants.ECHO_STAGE_BUFFER_MILLIS);
        sourceNamePrefix = "{" + MantisSourceJobConstants.MANTIS_META_SOURCE_NAME + ":" + "\"" + clusterName + "\",";
        mantisEventEnvelopeReader = mapper.readerFor(MantisEventEnvelope.class);
    }

    private String insertSourceJobName(String event) {
        StringBuilder sb = new StringBuilder(sourceNamePrefix);
        int indexofbrace = event.indexOf('{');

        if (indexofbrace != -1) {
            event = sb.append(event.substring(indexofbrace + 1)).toString();
        }

        return event;
    }

    public Observable<String> call(Context context,
                                   Observable<String> events) {
        return events
            .buffer(bufferDuration, TimeUnit.MILLISECONDS)
            .flatMapIterable(i -> i)
            .filter((event) -> !event.isEmpty())
            .flatMap((envelopeStr) -> {
                try {
                    MantisEventEnvelope envelope = mantisEventEnvelopeReader.readValue(envelopeStr);
                    return Observable.from(envelope.getEventList())
                        .map((event) -> event.getData());
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                    // Could not parse just send it along.
                    return Observable.just(envelopeStr);
                }
            })
            .map(this::insertSourceJobName)
            .onErrorResumeNext((t1) -> {
                LOGGER.error("Exception occurred in : " + clusterName + " error is " + t1.getMessage());
                return Observable.empty();
            });
    }

    public static List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> params = new ArrayList<>();

        // buffer duration
        params.add(new IntParameter()
                       .name(MantisSourceJobConstants.ECHO_STAGE_BUFFER_MILLIS)
                       .description("buffer time in millis")
                       .validator(Validators.range(100, 10000))
                       .defaultValue(250)
                       .build());

        return params;
    }

    public static ScalarToScalar.Config<String, String> config() {
        return new ScalarToScalar.Config<String, String>()
            .codec(Codecs.string())
            .concurrentInput()
            .withParameters(getParameters());
    }
}