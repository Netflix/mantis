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

package io.mantisrx.runtime.executor;

import java.util.LinkedList;
import java.util.List;

import io.mantisrx.runtime.api.Job;
import io.mantisrx.runtime.api.MantisJob;
import io.mantisrx.runtime.api.MantisJobProvider;
import io.mantisrx.runtime.api.ScalarToScalar;
import io.mantisrx.runtime.common.codec.Codecs;
import reactor.core.publisher.Flux;


public class TestJobSingleStage extends MantisJobProvider<Integer> {

    private List<Integer> itemsWritten = new LinkedList<>();

    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new TestJobSingleStage().getJobInstance());
    }

    public List<Integer> getItemsWritten() {
        return itemsWritten;
    }

    @Override
    public Job<Integer> getJobInstance() {
        return MantisJob
                .source((t1, t2) -> Flux.just(Flux.range(0, 10)))
                // doubles number
                .stage((context, t1) -> Flux.from(t1).map(t11 -> t11 * t11),
                    new ScalarToScalar.Config<Integer, Integer>().codec(Codecs.integer()))
                .sink((context, portRequest, publisher) ->
                    Flux.from(publisher)
                        .doOnNext(t1 -> {
                            System.out.println(t1);
                            itemsWritten.add(t1);
                        })
                        .blockLast())
                .create();
    }

}
