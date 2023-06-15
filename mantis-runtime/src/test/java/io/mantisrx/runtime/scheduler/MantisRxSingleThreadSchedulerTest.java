/*
 * Copyright 2021 Netflix, Inc.
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
package io.mantisrx.runtime.scheduler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.mantisrx.common.metrics.rx.MonitorOperator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.internal.util.RxThreadFactory;


public class MantisRxSingleThreadSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(MantisRxSingleThreadSchedulerTest.class);

    Observable<Observable<String>> createSourceObs(int numInnerObs, int valuesPerCompletingInnerObs) {
        return Observable.range(1, numInnerObs)
            .map(x -> {
                if (x != 1) {
                    return Observable.interval(10, TimeUnit.MILLISECONDS)
                        .map(l -> String.format("I%d: %d", x, l.intValue()))
                        .take(valuesPerCompletingInnerObs);
                } else {
                    return Observable.interval(100, TimeUnit.MILLISECONDS)
                        .map(l -> String.format("I%d: %d", x, l.intValue()));
                }
            });
    }

    @Test
    public void testObserveOnAfterOnCompleteMantisRxScheduler() throws InterruptedException {
        int nThreads = 6;
        final MantisRxSingleThreadScheduler[] mantisRxSingleThreadSchedulers = new MantisRxSingleThreadScheduler[nThreads];
        RxThreadFactory rxThreadFactory = new RxThreadFactory("MantisRxScheduler-");
        logger.info("creating {} Mantis threads", nThreads);
        for (int i = 0; i < nThreads; i++) {
            mantisRxSingleThreadSchedulers[i] = new MantisRxSingleThreadScheduler(rxThreadFactory);
        }

        int numInnerObs = 10;
        int valuesPerCompletingInnerObs = 2;
        int valuesToWaitFor = numInnerObs*valuesPerCompletingInnerObs + 10;
        Observable<Observable<String>> oo = createSourceObs(numInnerObs, valuesPerCompletingInnerObs);

        final CountDownLatch latch = new CountDownLatch(valuesToWaitFor);
        Observable<String> map = oo
            .lift(new MonitorOperator<>("worker_stage_outer"))
            .map(observable -> observable
                .groupBy(e -> System.nanoTime() % nThreads)
                .flatMap(go -> go
                    .observeOn(mantisRxSingleThreadSchedulers[go.getKey().intValue()])
                    .doOnNext(x -> {
                        logger.info("processing {} on thread {}", x, Thread.currentThread().getName());
                        latch.countDown();
                    })
                )
            ).flatMap(x -> x);

        Subscription subscription = map.subscribe();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        for (int i = 0; i < nThreads; i++) {
            assertFalse(mantisRxSingleThreadSchedulers[i].createWorker().isUnsubscribed());
        }
        subscription.unsubscribe();
    }
}
