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

import com.netflix.mantis.samples.proto.AggregationReport;
import com.netflix.mantis.samples.proto.RequestAggregation;
import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


/**
 * This is the final stage of this 3 stage job. It receives events from {@link AggregationStage}
 * The role of this stage is to collect aggregates generated by the previous stage for all the groups within
 * a window and generate a unified report of them.
 */
@Slf4j
public class CollectStage implements ScalarComputation<RequestAggregation,String> {
    private static final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Observable<String> call(Context context, Observable<RequestAggregation> requestAggregationO) {
        return requestAggregationO
                .window(5, TimeUnit.SECONDS)
                .flatMap((requestAggO) -> requestAggO
                        .reduce(new RequestAggregationAccumulator(),(acc, requestAgg) -> acc.addAggregation(requestAgg))
                        .map(RequestAggregationAccumulator::generateReport)
                        .doOnNext((report) -> {
                            log.debug("Generated Collection report {}", report);
                        })
                )
                .map((report) -> {
                    try {
                        return mapper.writeValueAsString(report);
                    } catch (JsonProcessingException e) {
                        log.error(e.getMessage());
                        return null;
                    }
                }).filter(Objects::nonNull);
    }

    @Override
    public void init(Context context) {

    }

    public static ScalarToScalar.Config<RequestAggregation,String> config(){
        return new ScalarToScalar.Config<RequestAggregation,String>()
                .codec(Codecs.string());
    }

    /**
     * The accumulator class as the name suggests accumulates all aggregates seen during a window and
     * generates a consolidated report at the end.
     */
    static class RequestAggregationAccumulator {
        private final Map<String, Integer> pathToCountMap = new HashMap<>();

        public RequestAggregationAccumulator()  {}

        public RequestAggregationAccumulator addAggregation(RequestAggregation agg) {
            pathToCountMap.put(agg.getPath(), agg.getCount());
            return this;
        }

        public AggregationReport generateReport() {
            log.info("Generated report from=> {}", pathToCountMap);
            return new AggregationReport(pathToCountMap);
        }
    }
}
