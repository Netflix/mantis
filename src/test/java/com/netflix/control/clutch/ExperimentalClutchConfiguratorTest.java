package com.netflix.control.clutch;

import com.netflix.control.clutch.metrics.IClutchMetricsRegistry;
import org.assertj.core.data.Offset;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

// TODO: These RxJava tests are extremely flaky when using the timers.
public class ExperimentalClutchConfiguratorTest {

    private static Random random = ThreadLocalRandom.current();

    public static double getRandomNear(double mean, double maxNosie) {
        return mean + (maxNosie * random.nextDouble());
    }

    @Ignore
    @Test public void shouldEmitConfigAfterInitialMillis() {

        TestSubscriber<ClutchConfiguration> subscriber = new TestSubscriber<>();

        ExperimentalClutchConfigurator ecc = new ExperimentalClutchConfigurator(new IClutchMetricsRegistry() { },
                1, 10, Observable.empty(), 10);

        Observable<Event> data = Observable.interval(1, TimeUnit.MILLISECONDS)
                .take(100) // 100 ms
                .map(__ -> new Event(Clutch.Metric.RPS, getRandomNear(50.0, 5.0)));

        data.compose(ecc)
                .subscribe(subscriber);

        subscriber.awaitTerminalEvent(110, TimeUnit.MILLISECONDS);
        subscriber.assertValueCount(1);
        subscriber.assertNoErrors();
        assertThat(subscriber.getOnNextEvents().get(0).setPoint)
                .isCloseTo(50 * 0.6, Offset.offset(5.0));
    }

    @Ignore
    @Test public void shouldEmitConfigOnTimerTick() {
        TestSubscriber<ClutchConfiguration> subscriber = new TestSubscriber<>();
        Subject<Long, Long> timer = BehaviorSubject.create();

        ExperimentalClutchConfigurator ecc = new ExperimentalClutchConfigurator(new IClutchMetricsRegistry() { },
                1, 10, timer, 10);

        Observable<Event> data = Observable.interval(1, TimeUnit.MILLISECONDS)
                .take(100) // 100 ms
                .map(__ -> new Event(Clutch.Metric.RPS, getRandomNear(50.0, 5.0)));

        data.compose(ecc)
                .subscribe(subscriber);
        timer.onNext(1L);

        subscriber.awaitTerminalEvent(150, TimeUnit.MILLISECONDS);
        System.out.println(subscriber.getOnNextEvents());
        subscriber.assertValueCount(2);
        assertThat(subscriber.getOnNextEvents().get(0).setPoint)
                .isCloseTo(50 * 0.6, Offset.offset(5.0));
        assertThat(subscriber.getOnNextEvents().get(1).setPoint)
                .isCloseTo(50 * 0.6, Offset.offset(5.0));
    }

    @Ignore
    @Test public void shoudlReconfigureOnMetricChange() {
        TestSubscriber<ClutchConfiguration> subscriber = new TestSubscriber<>();
        Observable<Long> timer = Observable.interval(150, TimeUnit.MILLISECONDS);

        ExperimentalClutchConfigurator ecc = new ExperimentalClutchConfigurator(new IClutchMetricsRegistry() { },
                1, 10, timer, 10);

        Observable<Event> data = Observable.interval(1, TimeUnit.MILLISECONDS)
                .take(100) // 100 ms
                .map(__ -> new Event(Clutch.Metric.RPS, getRandomNear(50.0, 5.0)));
        Observable<Event> data2 = Observable.interval(1, TimeUnit.MILLISECONDS)
                .take(500) // 500 ms
                .map(__ -> new Event(Clutch.Metric.RPS, getRandomNear(80.0, 5.0)));

        data.concatWith(data2)
                .compose(ecc)
                .subscribe(subscriber);

        subscriber.awaitTerminalEvent(550, TimeUnit.MILLISECONDS);
        assertThat(subscriber.getValueCount()).isGreaterThanOrEqualTo(3);

        // Initial Config
        assertThat(subscriber.getOnNextEvents().get(0).setPoint)
                .isCloseTo(50 * 0.6, Offset.offset(5.0));

        // First Config
        assertThat(subscriber.getOnNextEvents().get(1).setPoint)
                .isCloseTo(50, Offset.offset(5.0));

        // Second Config
        assertThat(subscriber.getOnNextEvents().get(1).setPoint)
                .isCloseTo(80 * 0.6, Offset.offset(5.0));
    }
}
