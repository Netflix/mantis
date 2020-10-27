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

package io.mantisrx.runtime.rx;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;


public class MonitorOperator<T> implements Operator<T, T> {

    private static Logger logger = LoggerFactory.getLogger(MonitorOperator.class);
    private final Counter next;
    private final Gauge nextGauge;
    private final Gauge error;
    private final Gauge complete;
    private final Gauge subscribe;
    private String name;

    public MonitorOperator(String name) {
        this.name = name;
        Metrics m =
                new Metrics.Builder()
                        .name(name)
                        .addCounter("onNext")
                        .addGauge("onError")
                        .addGauge("onComplete")
                        .addGauge("subscribe")
                        .addGauge("onNextGauge")
                        .build();

        m = MetricsRegistry.getInstance().registerAndGet(m);

        next = m.getCounter("onNext");
        error = m.getGauge("onError");
        complete = m.getGauge("onComplete");
        subscribe = m.getGauge("subscribe");
        nextGauge = m.getGauge("onNextGauge");
    }

    @Override
    public Subscriber<? super T> call(final Subscriber<? super T> o) {
        subscribe.increment();

        o.add(Subscriptions.create(new Action0() {
            @Override
            public void call() {
                subscribe.decrement();
            }
        }));
        return new Subscriber<T>(o) {

            @Override
            public void onCompleted() {
                logger.debug("onCompleted() called for monitored observable with name: " + name);
                complete.increment();
                o.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                logger.error("onError() called for monitored observable with name: " + name, e);
                error.increment();
                o.onError(e);
            }

            @Override
            public void onNext(T t) {
                next.increment();
                nextGauge.set(next.value());
                o.onNext(t);
            }
        };
    }
}
