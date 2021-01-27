package io.mantisrx.runtime.scheduler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.mantisrx.common.metrics.rx.MonitorOperator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.internal.util.RxThreadFactory;


public class MantisRxSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(MantisRxSchedulerTest.class);

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
        final MantisRxScheduler[] mantisRxSchedulers = new MantisRxScheduler[nThreads];
        RxThreadFactory rxThreadFactory = new RxThreadFactory("MantisRxScheduler-");
        logger.info("creating {} Mantis threads", nThreads);
        for (int i = 0; i < nThreads; i++) {
            mantisRxSchedulers[i] = new MantisRxScheduler(rxThreadFactory);
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
                    .observeOn(mantisRxSchedulers[go.getKey().intValue()])
                    .doOnNext(x -> {
                        logger.info("processing {} on thread {}", x, Thread.currentThread().getName());
                        latch.countDown();
                    })
                )
            ).flatMap(x -> x);

        Subscription subscription = map.subscribe();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        for (int i = 0; i < nThreads; i++) {
            assertFalse(mantisRxSchedulers[i].createWorker().isUnsubscribed());
        }
        subscription.unsubscribe();
    }
}
