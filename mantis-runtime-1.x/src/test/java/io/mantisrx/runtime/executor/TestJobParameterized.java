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

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;


public class TestJobParameterized extends MantisJobProvider<Integer> {

    private List<Integer> itemsWritten = new LinkedList<Integer>();

    public static void main(String[] args) throws InterruptedException {

        Job<Integer> job = new TestJobParameterized().getJobInstance();

        LocalJobExecutorNetworked.execute(job,
                new Parameter("start-range", "1"),
                new Parameter("end-range", "100"),
                new Parameter("scale-by", "2"));
    }

    public List<Integer> getItemsWritten() {
        return itemsWritten;
    }

    @Override
    public Job<Integer> getJobInstance() {
        return MantisJob
                .<Integer>
                        source(new Source<Integer>() {
                    @Override
                    public Observable<Observable<Integer>> call(Context context,
                                                                Index index) {
                        Integer start = (Integer) context.getParameters().get("start-range");
                        Integer end = (Integer) context.getParameters().get("end-range");
                        return Observable.just(Observable.range(start, end));
                    }
                })
                // doubles number
                .stage(new ScalarComputation<Integer, Integer>() {
                    @Override
                    public Observable<Integer> call(Context context, Observable<Integer> t1) {
                        final Integer scale = (Integer) context.getParameters().get("scale-by");
                        return t1.map(new Func1<Integer, Integer>() {
                            @Override
                            public Integer call(Integer t1) {
                                return t1 * scale;
                            }
                        });
                    }
                }, new ScalarToScalar.Config<Integer, Integer>()
                        .codec(Codecs.integer()))
                .sink(new Sink<Integer>() {
                    @Override
                    public void init(Context context) {
                        System.out.println("sink init called");
                    }
                    @Override
                    public void call(Context context,
                                     PortRequest p,
                                     Observable<Integer> o) {
                        final String message = (String) context.getParameters().get("sink-message");
                        o
                                .toBlocking().forEach(new Action1<Integer>() {
                            @Override
                            public void call(Integer t1) {
                                System.out.println(message + t1);
                                itemsWritten.add(t1);
                            }
                        });
                    }
                })
                .metadata(new Metadata.Builder()
                        .name("test job")
                        .description("showcase parameters")
                        .build())
                .parameterDefinition(new IntParameter()
                        .name("start-range")
                        .validator(Validators.range(0, 100))
                        .build())
                .parameterDefinition(new IntParameter()
                        .name("end-range")
                        .validator(Validators.range(100, 1000))
                        .build())
                .parameterDefinition(new IntParameter()
                        .name("scale-by")
                        .description("scale each value from the range")
                        .validator(Validators.range(1, 10))
                        .build())
                .parameterDefinition(new StringParameter()
                        .name("sink-message")
                        .defaultValue("hello: ")
                        .description("concat with results")
                        .validator(Validators.notNullOrEmpty())
                        .build())
                .create();
    }

}
