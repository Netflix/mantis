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

package io.reactivex.mantis.remote.observable;

import java.util.Map;

import io.mantisrx.common.codec.Encoder;
import io.reactivex.mantis.remote.observable.filter.ServerSideFilters;
import io.reactivex.mantis.remote.observable.slotting.RoundRobin;
import io.reactivex.mantis.remote.observable.slotting.SlottingStrategy;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;


public class ServeNestedObservable<T> extends ServeConfig<T, T> {

    private Encoder<T> encoder;

    public ServeNestedObservable(Builder<T> builder) {
        super(builder.name, builder.slottingStrategy, builder.filterFunction,
                builder.maxWriteAttempts);
        this.encoder = builder.encoder;
        applySlottingSideEffectToObservable(builder.observable);
    }

    private void applySlottingSideEffectToObservable(Observable<Observable<T>> o) {

        final Observable<T> withSideEffects =
                Observable.merge(o)
                        .doOnEach(new Observer<T>() {
                            @Override
                            public void onCompleted() {
                                slottingStrategy.completeAllConnections();
                            }

                            @Override
                            public void onError(Throwable e) {
                                slottingStrategy.errorAllConnections(e);
                            }

                            @Override
                            public void onNext(T value) {
                                slottingStrategy.writeOnSlot(null, value);
                            }
                        });

        final MutableReference<Subscription> subscriptionRef = new MutableReference<>();
        slottingStrategy.registerDoAfterFirstConnectionAdded(new Action0() {
            @Override
            public void call() {
                subscriptionRef.setValue(withSideEffects.subscribe());
            }
        });

        slottingStrategy.registerDoAfterLastConnectionRemoved(new Action0() {
            @Override
            public void call() {
                subscriptionRef.getValue().unsubscribe();
            }
        });
    }

    public Encoder<T> getEncoder() {
        return encoder;
    }

    public static class Builder<T> {

        private String name;
        private Observable<Observable<T>> observable;
        private SlottingStrategy<T> slottingStrategy = new RoundRobin<>();
        private Encoder<T> encoder;
        private Func1<Map<String, String>, Func1<T, Boolean>> filterFunction = ServerSideFilters.noFiltering();
        private int maxWriteAttempts = 3;

        public Builder<T> name(String name) {
            if (name != null && name.length() > 127) {
                throw new IllegalArgumentException("Observable name must be less than 127 characters");
            }
            this.name = name;
            return this;
        }

        public Builder<T> observable(Observable<Observable<T>> observable) {
            this.observable = observable;
            return this;
        }

        public Builder<T> maxWriteAttempts(int maxWriteAttempts) {
            this.maxWriteAttempts = maxWriteAttempts;
            return this;
        }


        public Builder<T> slottingStrategy(SlottingStrategy<T> slottingStrategy) {
            this.slottingStrategy = slottingStrategy;
            return this;
        }

        public Builder<T> encoder(Encoder<T> encoder) {
            this.encoder = encoder;
            return this;
        }

        public Builder<T> serverSideFilter(
                Func1<Map<String, String>, Func1<T, Boolean>> filterFunc) {
            this.filterFunction = filterFunc;
            return this;
        }

        public ServeNestedObservable<T> build() {
            return new ServeNestedObservable<T>(this);
        }
    }
}
