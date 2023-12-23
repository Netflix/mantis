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

import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.Operator;
import rx.Producer;
import rx.Subscriber;
import rx.subscriptions.Subscriptions;


public class DropOperator<T> implements Operator<T, T> {

    public static final String METRIC_GROUP = "DropOperator";
    private static final Logger logger = LoggerFactory.getLogger(DropOperator.class);
    private final Counter next;
    private final Counter error;
    private final Counter complete;
    private final Counter dropped;
    MetricGroupId metricGroupId;


    public DropOperator(MeterRegistry meterRegistry) {
        next = meterRegistry.counter(METRIC_GROUP+ "_" +"" + Counters.onNext);
        error = meterRegistry.counter(METRIC_GROUP+ "_" +"" + Counters.onError);
        complete = meterRegistry.counter(METRIC_GROUP+ "_" +"" + Counters.onComplete);
        dropped = meterRegistry.counter(METRIC_GROUP+ "_" +"" + Counters.dropped);
    }


    public DropOperator(MeterRegistry meterRegistry, final String name) {
        next = meterRegistry.counter(METRIC_GROUP+ "_" + name +"" + Counters.onNext);
        error = meterRegistry.counter(METRIC_GROUP+ "_" + name +"" + Counters.onError);
        complete = meterRegistry.counter(METRIC_GROUP+ "_" + name +"" + Counters.onComplete);
        dropped = meterRegistry.counter(METRIC_GROUP+ "_" + name +"" + Counters.dropped);
    }

    public DropOperator(MeterRegistry meterRegistry, final String name, final Tags tags) {
        next = Counter.builder(METRIC_GROUP+ "_" + name +"" + Counters.onNext)
            .tags(tags)
            .register(meterRegistry);
        error = Counter.builder(METRIC_GROUP+ "_" + name +"" + Counters.onError)
            .tags(tags)
            .register(meterRegistry);
        complete = Counter.builder(METRIC_GROUP+ "_" + name +"" + Counters.onComplete)
            .tags(tags)
            .register(meterRegistry);
        dropped = Counter.builder(METRIC_GROUP+ "_" + name +"" + Counters.dropped)
            .tags(tags)
            .register(meterRegistry);
    }

    @Override
    public Subscriber<? super T> call(final Subscriber<? super T> o) {
        final AtomicLong requested = new AtomicLong();

        o.add(Subscriptions.create(() -> {

        }));
        o.setProducer(new Producer() {
            @Override
            public void request(long n) {
                if (requested.get() == Long.MAX_VALUE) {
                    logger.warn("current requested is int max do not increment");
                } else {
                    requested.getAndAdd(n);

                }

            }
        });
        return new Subscriber<T>(o) {
            @Override
            public void onCompleted() {
                complete.increment();
                o.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                error.increment();
                logger.error("onError() occured in DropOperator for groupId: {}", METRIC_GROUP, e);
                o.onError(e);
            }

            @Override
            public void onNext(T t) {

                if (requested.get() > 0) {
                    requested.decrementAndGet();
                    o.onNext(t);
                    next.increment();
                } else {

                    dropped.increment();
                }

            }

            @Override
            public void setProducer(Producer p) {
                p.request(Long.MAX_VALUE);
            }
        };
    }

    public enum Counters {
        onNext,
        onError,
        onComplete,
        dropped
    }

    public enum Gauges {
        subscribe,
        requested
    }


}
