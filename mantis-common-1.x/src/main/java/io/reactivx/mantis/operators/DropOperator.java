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

import com.netflix.spectator.api.Tag;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.Operator;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;


public class DropOperator<T> implements Operator<T, T> {

    public static final String METRIC_GROUP = "DropOperator";
    private static final Logger logger = LoggerFactory.getLogger(DropOperator.class);
    private final Counter next;
    private final Counter error;
    private final Counter complete;
    private final Counter dropped;
    MetricGroupId metricGroupId;

    public DropOperator(final Metrics m) {
        next = m.getCounter("" + Counters.onNext);
        error = m.getCounter("" + Counters.onError);
        complete = m.getCounter("" + Counters.onComplete);
        dropped = m.getCounter("" + Counters.dropped);
    }
    public DropOperator(final MetricGroupId groupId) {
        this.metricGroupId = groupId;

        Metrics m = new Metrics.Builder()
                .id(metricGroupId)
                .addCounter("" + Counters.onNext)
                .addCounter("" + Counters.onError)
                .addCounter("" + Counters.onComplete)
                .addCounter("" + Counters.dropped)
                .build();

        m = MetricsRegistry.getInstance().registerAndGet(m);

        next = m.getCounter("" + Counters.onNext);
        error = m.getCounter("" + Counters.onError);
        complete = m.getCounter("" + Counters.onComplete);
        dropped = m.getCounter("" + Counters.dropped);
    }

    public DropOperator(final String name) {
        this(new MetricGroupId(METRIC_GROUP + "_" + name));
    }

    public DropOperator(final String name, final Tag... tags) {
        this(new MetricGroupId(METRIC_GROUP + "_" + name, tags));
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
                logger.error("onError() occured in DropOperator for groupId: {}", metricGroupId.id(), e);
                o.onError(e);
            }

            @Override
            public void onNext(T t) {

                if (requested.get() > 0) {
                    o.onNext(t);
                    next.increment();
                    requested.decrementAndGet();

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
