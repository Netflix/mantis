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

package io.mantisrx.runtime.reactor;

import java.util.function.BiFunction;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;


public class MonitorOperator<T> {
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

    public BiFunction<Scannable, ? super CoreSubscriber<? super T>, ? extends CoreSubscriber<? super T>> operator() {
        return (BiFunction<Scannable, CoreSubscriber<? super T>, CoreSubscriber<? super T>>) (scannable, actual) -> new CoreSubscriber<T>() {

            @Override
            public void onSubscribe(Subscription s) {
                logger.trace("[{}] onSubscribe", name);
                subscribe.increment();
                actual.onSubscribe(s);
            }

            @Override
            public void onNext(T t) {
                logger.trace("[{}] onNext for {}", name, t);
                next.increment();
                nextGauge.set(next.value());
                actual.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                logger.trace("[{}] onError ", name, t);
                error.increment();
                actual.onError(t);
            }

            @Override
            public void onComplete() {
                logger.trace("[{}] onComplete", name);
                complete.increment();
                actual.onComplete();
            }
        };
    }
}
