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
import io.mantisrx.runtime.KeyToKey;
import io.mantisrx.runtime.KeyToScalar;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.ScalarToKey;
import io.mantisrx.runtime.codec.JacksonCodecs;
import io.mantisrx.runtime.computation.KeyComputation;
import io.mantisrx.runtime.computation.ToKeyComputation;
import io.mantisrx.runtime.computation.ToScalarComputation;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.source.Sources;
import io.reactivx.mantis.operators.GroupedObservableUtils;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.GroupedObservable;
import rx.schedulers.Schedulers;


public class TestGroupByJob extends MantisJobProvider<Pair> {

    private List<Pair> itemsWritten = new LinkedList<>();

    public static void main(String[] args) {
        LocalJobExecutorNetworked.execute(new TestGroupByJob().getJobInstance());
    }

    public List<Pair> getItemsWritten() {
        return itemsWritten;
    }

    @Override
    public Job<Pair> getJobInstance() {
        return MantisJob
                .<Integer>
                        source(Sources.observable(Observable.range(0, 100).subscribeOn(Schedulers.io())))
                // group by even/odd
                .stage(new ToKeyComputation<Integer, String, Integer>() {
                    @Override
                    public Observable<GroupedObservable<String, Integer>> call(
                            Context context,
                            Observable<Integer> t1) {
                        return t1.groupBy(new Func1<Integer, String>() {
                            @Override
                            public String call(Integer t1) {
                                if ((t1 % 2) == 0) {
                                    return "even";
                                } else {
                                    return "odd";
                                }
                            }
                        });
                    }
                }, new ScalarToKey.Config<Integer, String, Integer>()
                        .codec(Codecs.integer()))
                // double numbers
                .stage(new KeyComputation<String, Integer, String, Integer>() {
                    @Override
                    public Observable<GroupedObservable<String, Integer>> call(
                            Context context,
                            final GroupedObservable<String, Integer> group) {

                        // return group with doubled numbers
                        return
                                Observable.just(GroupedObservableUtils
                                        .createGroupedObservable(group.getKey(),
                                                group.map(new Func1<Integer, Integer>() {
                                                    @Override
                                                    public Integer call(Integer t1) {
                                                        return t1 * t1;
                                                    }
                                                })));
                    }
                }, new KeyToKey.Config<String, Integer, String, Integer>()
                        .codec(Codecs.integer()))
                // create a new type
                .stage(new ToScalarComputation<String, Integer, Pair>() {
                    @Override
                    public Observable<Pair> call(Context t1,
                                                 final GroupedObservable<String, Integer> group) {
                        System.out.println("group computation running on thread: " + Thread.currentThread().getName() + " group: " + group.getKey());

                        return group.map(new Func1<Integer, Pair>() {
                            @Override
                            public Pair call(Integer t1) {
                                return new Pair(group.getKey(), t1);
                            }
                        });
                    }

                }, new KeyToScalar.Config<String, Integer, Pair>()
                        .codec(JacksonCodecs.pojo(Pair.class)))
                .sink(new Sink<Pair>() {
                    @Override
                    public void call(Context t1, PortRequest p, Observable<Pair> o) {
                        o.toBlocking()
                                .forEach(new Action1<Pair>() {
                                    @Override
                                    public void call(Pair pair) {
                                        System.out.println(pair);
                                        itemsWritten.add(pair);
                                    }
                                });
                    }
                })
                .create();
    }

}
