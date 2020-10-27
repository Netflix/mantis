/*
 *
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.mantisrx.runtime.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import io.mantisrx.runtime.api.computation.ScalarComputation;
import io.mantisrx.runtime.api.lifecycle.LifecycleNoOp;
import io.mantisrx.runtime.api.source.Source;
import io.mantisrx.runtime.common.codec.Codecs;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;


public class testMantisJob {
    @Test
    public void testMantisJob() {
        List<Integer> sinkOutput = new ArrayList<>();
        Job<Integer> testJob = MantisJob
            // source that emits integers 1 to 3
            .source((c, i) -> Flux.just(Flux.range(1, 3)))
            // stage multiples each int by 100
            .stage((c, p) -> Flux.from(p).map(i -> i * 100),
                new ScalarToScalar.Config<Integer, Integer>().codec(Codecs.integer()))
            .sink((context, portRequest, p) -> {
                Flux.from(p)
                    .doOnNext(i -> sinkOutput.add(i))
                    .subscribe();
                return;
            })
            .lifecycle(new LifecycleNoOp())
            .create();

        // verify Source fn from Job
        SourceHolder<?> source = testJob.getSource();
        Source<?> sourceFunction = source.getSourceFunction();
        Flux<? extends Publisher<?>> sourceFlux = Flux.from(sourceFunction.createDataSource(null, null));
        StepVerifier.create(sourceFlux
            .doOnNext(p -> StepVerifier.create(
                Flux.from((Publisher<Integer>) p))
                    .expectNext(1)
                    .expectNext(2)
                    .expectNext(3)
                    .verifyComplete()
        ))
        .expectNextCount(1)
        .verifyComplete();

        // verify stage Fn from Job
        assertEquals(1, testJob.getStages().size());
        StageConfig<?, ?> stageConfig = testJob.getStages().stream().findFirst().get();
        assertTrue(stageConfig instanceof ScalarToScalar);
        if (stageConfig instanceof ScalarToScalar) {
            ScalarToScalar<Integer, Integer> scalarStage = (ScalarToScalar<Integer, Integer>) stageConfig;
            ScalarComputation<Integer, Integer> computation = scalarStage.getComputation();
            StepVerifier.create(
                Flux.from(computation.apply(null, Flux.range(1, 3))))
                .expectNext(100)
                .expectNext(200)
                .expectNext(300)
                .verifyComplete();
        }
        // verify sink fn from Job
        testJob.getSink().getSinkAction().apply(null, null, Flux.fromStream(IntStream.of(100, 200, 300).boxed()));
        assertEquals(3, sinkOutput.size());
        assertEquals(new Integer(100), sinkOutput.get(0));
        assertEquals(new Integer(200), sinkOutput.get(1));
        assertEquals(new Integer(300), sinkOutput.get(2));
    }
}
