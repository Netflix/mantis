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

package io.reactivx.mantis.operators;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.Operator;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;


public class DisableBackPressureOperator<T> implements Operator<T, T> {


    private static final Logger logger = LoggerFactory.getLogger(DisableBackPressureOperator.class);

    public DisableBackPressureOperator() {

    }


    @Override
    public Subscriber<? super T> call(final Subscriber<? super T> o) {

        final AtomicLong requested = new AtomicLong();

        o.add(Subscriptions.create(new Action0() {
            @Override
            public void call() {

            }
        }));
        o.setProducer(new Producer() {
            @Override
            public void request(long n) {
            }
        });
        return new Subscriber<T>(o) {
            @Override
            public void onCompleted() {

                o.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                o.onError(e);
            }

            @Override
            public void onNext(T t) {

                o.onNext(t);
            }

            @Override
            public void setProducer(Producer p) {
                p.request(Long.MAX_VALUE);
            }
        };
    }


}
