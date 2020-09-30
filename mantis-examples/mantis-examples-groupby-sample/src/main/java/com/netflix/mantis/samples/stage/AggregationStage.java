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

package com.netflix.mantis.samples.stage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.mantis.samples.proto.RequestAggregation;
import com.netflix.mantis.samples.proto.RequestEvent;
import io.mantisrx.common.MantisGroup;
import io.mantisrx.common.codec.Codec;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.GroupToScalar;
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.observables.GroupedObservable;


/**
 * This is the 2nd stage of this three stage job. It receives events from {@link GroupByStage}
 * This stage converts Grouped Events to Scalar events {@link GroupToScalarComputation} Typically used in the
 * reduce portion of a map reduce computation.
 *
 * This stage receives an <code>Observable<MantisGroup<String,RequestEvent>></code>. This represents a stream of
 * request events tagged by the URI Path they belong to.
 * This stage then groups the events by the path and and counts the number of invocations of each path over a window.
 */
@Slf4j
public class AggregationStage implements GroupToScalarComputation<String, RequestEvent, RequestAggregation> {

    public static final String AGGREGATION_DURATION_MSEC_PARAM = "AggregationDurationMsec";
    int aggregationDurationMsec;

    private Observable<? extends RequestAggregation> aggregate(GroupedObservable<String, MantisGroup<String, RequestEvent>> go) {
        return go.reduce(RequestAggregation.builder().build(), (accumulator, value) -> {
            accumulator.setCount(accumulator.getCount() + value.getValue().getLatency());
            accumulator.setPath(go.getKey());
            return accumulator;
        })
                //                                .map((count) -> RequestAggregation.builder().count(count).path(go.getKey()).build())
                .doOnNext((aggregate) -> {
                    log.debug("Generated aggregate {}", aggregate);
                });
    }

    /**
     * The aggregate method is invoked by the Mantis runtime while executing the job.
     * @param context Provides metadata information related to the current job.
     * @param mantisGroupO This is an Observable of {@link MantisGroup} events. Each event is a pair of the Key -> uri Path and
     *                     the {@link RequestEvent} event itself.
     * @return
     */
    @Override
    public Observable<RequestAggregation> call(Context context, Observable<MantisGroup<String, RequestEvent>> mantisGroupO) {
        return mantisGroupO
                .window(aggregationDurationMsec, TimeUnit.MILLISECONDS)
                .flatMap((omg) -> omg.groupBy(MantisGroup::getKeyValue)
                        .flatMap(//                                .map((count) -> RequestAggregation.builder().count(count).path(go.getKey()).build())
                                this::aggregate
                        ));
    }

    /**
     * Invoked only once during job startup. A good place to add one time initialization actions.
     * @param context
     */
    @Override
    public void init(Context context) {
        aggregationDurationMsec = (int)context.getParameters().get(AGGREGATION_DURATION_MSEC_PARAM, 1000);
    }

    /**
     * Provides the Mantis runtime configuration information about the type of computation done by this stage.
     * E.g in this case it specifies this is a GroupToScalar computation and also provides a {@link Codec} on how to
     * serialize the {@link RequestAggregation} events before sending it to the {@link CollectStage}
     * @return
     */
    public static GroupToScalar.Config<String, RequestEvent, RequestAggregation> config(){
        return new GroupToScalar.Config<String, RequestEvent,RequestAggregation>()
                .description("sum events for a path")
                .codec(RequestAggregation.requestAggregationCodec())
                .withParameters(getParameters());
    }

    /**
     * Here we declare stage specific parameters.
     * @return
     */
    public static List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> params = new ArrayList<>();

        // Aggregation duration
        params.add(new IntParameter()
                .name(AGGREGATION_DURATION_MSEC_PARAM)
                .description("window size for aggregation")
                .validator(Validators.range(100, 10000))
                .defaultValue(5000)
                .build())	;

        return params;
    }

}
