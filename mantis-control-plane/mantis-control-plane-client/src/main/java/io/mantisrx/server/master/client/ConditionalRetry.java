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

package io.mantisrx.server.master.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.mantisrx.common.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;


public class ConditionalRetry {

    private static final Logger logger = LoggerFactory.getLogger(ConditionalRetry.class);
    private final Counter counter;
    private final String name;
    private final AtomicReference<Throwable> errorRef = new AtomicReference<>(null);
    private final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic;

    public ConditionalRetry(Counter counter, String name) {
        this(counter, name, Integer.MAX_VALUE);
    }

    public ConditionalRetry(Counter counter, String name, final int max) {
        this.counter = counter;
        this.name = name;
        this.retryLogic =
                new Func1<Observable<? extends Throwable>, Observable<?>>() {
                    @Override
                    public Observable<?> call(Observable<? extends Throwable> attempts) {
                        return attempts
                                .zipWith(Observable.range(1, max), new Func2<Throwable, Integer, Integer>() {
                                    @Override
                                    public Integer call(Throwable t1, Integer integer) {
                                        return integer;
                                    }
                                })
                                .flatMap(new Func1<Integer, Observable<?>>() {
                                    @Override
                                    public Observable<?> call(Integer integer) {
                                        if (errorRef.get() != null)
                                            return Observable.error(errorRef.get());
                                        if (ConditionalRetry.this.counter != null)
                                            ConditionalRetry.this.counter.increment();
                                        long delay = 2 * (integer > 10 ? 10 : integer);
                                        logger.info(": retrying " + ConditionalRetry.this.name +
                                                " after sleeping for " + delay + " secs");
                                        return Observable.timer(delay, TimeUnit.SECONDS);
                                    }
                                });
                    }
                };
    }

    public void setErrorRef(Throwable error) {
        errorRef.set(error);
    }

    public Counter getCounter() {
        return counter;
    }

    public Func1<Observable<? extends Throwable>, Observable<?>> getRetryLogic() {
        return retryLogic;
    }
}
