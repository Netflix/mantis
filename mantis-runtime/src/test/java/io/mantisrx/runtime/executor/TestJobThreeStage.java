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
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;


public class TestJobThreeStage extends MantisJobProvider<Integer> {

    private List<Integer> itemsWritten = new LinkedList<Integer>();

    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new TestJobThreeStage().getJobInstance());
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
                    public Observable<Observable<Integer>> call(Context t1,
                                                                Index t2) {
                        return Observable.just(Observable.range(0, 10));
                    }
                })
                // doubles number
                .stage(new ScalarComputation<Integer, Integer>() {
                    @Override
                    public Observable<Integer> call(Context context, Observable<Integer> t1) {
                        return t1.map(new Func1<Integer, Integer>() {
                            @Override
                            public Integer call(Integer t1) {
                                return t1 * t1;
                            }
                        });
                    }
                }, new ScalarToScalar.Config<Integer, Integer>()
                        .codec(Codecs.integer()))
                .stage(new ScalarComputation<Integer, Integer>() {
                    @Override
                    public Observable<Integer> call(Context context, Observable<Integer> t1) {
                        return t1.map(new Func1<Integer, Integer>() {
                            @Override
                            public Integer call(Integer t1) {
                                return t1 * t1;
                            }
                        });
                    }
                }, new ScalarToScalar.Config<Integer, Integer>()
                        .codec(Codecs.integer()))
                // return only even numbers
                .stage(new ScalarComputation<Integer, Integer>() {
                    @Override
                    public Observable<Integer> call(Context context, Observable<Integer> t1) {
                        return t1.filter(new Func1<Integer, Boolean>() {
                            @Override
                            public Boolean call(Integer t1) {
                                return ((t1 % 2) == 0);
                            }
                        });
                    }
                }, new ScalarToScalar.Config<Integer, Integer>()
                        .codec(Codecs.integer()))
                .sink(new Sink<Integer>() {
                    @Override
                    public void call(Context context,
                                     PortRequest p,
                                     Observable<Integer> o) {
                        o
                                .toBlocking().forEach(new Action1<Integer>() {
                            @Override
                            public void call(Integer t1) {
                                System.out.println(t1);
                                itemsWritten.add(t1);
                            }
                        });
                    }
                })
                .create();
    }

}
