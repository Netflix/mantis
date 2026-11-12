/*
 * Copyright 2026 Netflix, Inc.
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

package io.reactivex.mantis.network.push;

import static org.junit.jupiter.api.Assertions.*;
import static io.reactivex.mantis.network.push.ServerConfig.ConnectionBufferMode.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

class ConnectionBufferTest {
    @Test
    void activityStartsAtFirstChunkWithoutResettingDeadline() {
        for (int interval : new int[] {100, 150, 200}) {
            TestScheduler clock = new TestScheduler();
            PublishSubject<Integer> source = PublishSubject.create();
            TestSubscriber<List<Integer>> result = new TestSubscriber<>();
            source.compose(new ConnectionBuffer<Integer>(ACTIVITY, interval, clock)).subscribe(result);
            clock.advanceTimeBy(1, TimeUnit.DAYS);
            result.assertNoValues();
            source.onNext(1);
            clock.advanceTimeBy(interval - 1, TimeUnit.MILLISECONDS);
            source.onNext(2);
            result.assertNoValues();
            clock.advanceTimeBy(1, TimeUnit.MILLISECONDS);
            result.assertValue(Arrays.asList(1, 2));
            clock.advanceTimeBy(1, TimeUnit.DAYS);
            result.assertValueCount(1);
            source.onNext(3);
            clock.advanceTimeBy(interval, TimeUnit.MILLISECONDS);
            assertEquals(Arrays.asList(Arrays.asList(1, 2), Collections.singletonList(3)),
                    result.getOnNextEvents());
        }
    }

    @Test
    void noScheduledActionsWhileIdleOrAfterFlush() {
        TestScheduler clock = new TestScheduler();
        AtomicInteger scheduled = new AtomicInteger();
        Scheduler counting = new Scheduler() {
            @Override
            public Worker createWorker() {
                Worker delegate = clock.createWorker();
                return new Worker() {
                    @Override
                    public Subscription schedule(Action0 action) {
                        scheduled.incrementAndGet();
                        return delegate.schedule(action);
                    }
                    @Override
                    public Subscription schedule(Action0 action, long delay, TimeUnit unit) {
                        scheduled.incrementAndGet();
                        return delegate.schedule(action, delay, unit);
                    }
                    @Override
                    public void unsubscribe() { delegate.unsubscribe(); }
                    @Override
                    public boolean isUnsubscribed() { return delegate.isUnsubscribed(); }
                };
            }
        };
        PublishSubject<Integer> source = PublishSubject.create();
        Subscription subscription = source.compose(new ConnectionBuffer<Integer>(ACTIVITY, 100, counting))
                .subscribe();
        clock.advanceTimeBy(1, TimeUnit.DAYS);
        assertEquals(0, scheduled.get());
        source.onNext(1);
        source.onNext(2);
        assertEquals(1, scheduled.get());
        clock.advanceTimeBy(1, TimeUnit.DAYS);
        assertEquals(1, scheduled.get());
        subscription.unsubscribe();
    }

    @Test
    void periodicDefaultsAndCustomCadenceRemainPeriodic() {
        ServerConfig<Integer> defaults = new ServerConfig.Builder<Integer>().build();
        assertEquals(PERIODIC, defaults.getConnectionBufferMode());
        assertEquals(200, defaults.getConnectionBufferIntervalMs());
        for (int interval : new int[] {defaults.getConnectionBufferIntervalMs(), 150}) {
            TestScheduler clock = new TestScheduler();
            TestSubscriber<List<Integer>> result = new TestSubscriber<>();
            Observable.<Integer>never().compose(new ConnectionBuffer<Integer>(PERIODIC, interval, clock))
                    .subscribe(result);
            clock.advanceTimeBy(interval - 1, TimeUnit.MILLISECONDS);
            result.assertNoValues();
            clock.advanceTimeBy(1, TimeUnit.MILLISECONDS);
            result.assertValue(Collections.emptyList());
            clock.advanceTimeBy(interval, TimeUnit.MILLISECONDS);
            result.assertValueCount(2);
        }
    }

    @Test
    void completionFlushesErrorDiscardsAndCancellationStopsPendingWork() {
        for (int terminal = 0; terminal < 3; terminal++) {
            TestScheduler clock = new TestScheduler();
            PublishSubject<Integer> source = PublishSubject.create();
            TestSubscriber<List<Integer>> result = new TestSubscriber<>();
            source.compose(new ConnectionBuffer<Integer>(ACTIVITY, 100, clock)).subscribe(result);
            source.onNext(1);
            if (terminal == 0) {
                source.onCompleted();
                result.assertValue(Collections.singletonList(1));
                result.assertCompleted();
            } else if (terminal == 1) {
                source.onError(new IllegalStateException("synthetic failure"));
                result.assertError(IllegalStateException.class);
                result.assertNoValues();
            } else {
                result.unsubscribe();
            }
            int count = result.getOnNextEvents().size();
            clock.advanceTimeBy(1, TimeUnit.DAYS);
            result.assertValueCount(count);
            assertFalse(source.hasObservers());
        }
    }

    @Test
    void reusedTransformerHasIndependentConnectionState() {
        TestScheduler clock = new TestScheduler();
        ConnectionBuffer<Integer> buffer = new ConnectionBuffer<>(ACTIVITY, 100, clock);
        PublishSubject<Integer> first = PublishSubject.create();
        PublishSubject<Integer> second = PublishSubject.create();
        TestSubscriber<List<Integer>> a = new TestSubscriber<>();
        TestSubscriber<List<Integer>> b = new TestSubscriber<>();
        first.compose(buffer).subscribe(a);
        second.compose(buffer).subscribe(b);
        first.onNext(1);
        clock.advanceTimeBy(50, TimeUnit.MILLISECONDS);
        second.onNext(2);
        clock.advanceTimeBy(50, TimeUnit.MILLISECONDS);
        a.assertValue(Collections.singletonList(1));
        b.assertNoValues();
        clock.advanceTimeBy(50, TimeUnit.MILLISECONDS);
        b.assertValue(Collections.singletonList(2));
    }

    @Test
    void reentrantArrivalAndCompletionPreserveOrder() {
        TestScheduler clock = new TestScheduler();
        PublishSubject<Integer> source = PublishSubject.create();
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();
        source.compose(new ConnectionBuffer<Integer>(ACTIVITY, 100, clock)).subscribe(batch -> {
            received.addAll(batch);
            if (batch.contains(1)) {
                source.onNext(2);
                source.onCompleted();
            }
        }, error -> fail(error), () -> completed.set(true));
        source.onNext(1);
        clock.advanceTimeBy(100, TimeUnit.MILLISECONDS);
        assertEquals(Arrays.asList(1, 2), received);
        assertTrue(completed.get());
    }

    @Test
    void producerCanCompleteDuringBlockedTimerDeliveryWithoutInterruptingIt() throws Exception {
        PublishSubject<Integer> source = PublishSubject.create();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        Subscription subscription = source.compose(new ConnectionBuffer<Integer>(ACTIVITY, 1, Schedulers.computation()))
                .subscribe(batch -> {
                    received.addAll(batch);
                    if (batch.contains(1)) {
                        entered.countDown();
                        try {
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                errors.add(new AssertionError("delivery timed out"));
                            }
                        } catch (InterruptedException e) {
                            interrupted.set(true);
                        }
                    }
                }, error -> { errors.add(error); done.countDown(); }, done::countDown);
        try {
            source.onNext(1);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            source.onNext(2);
            source.onCompleted();
            assertEquals(Collections.singletonList(1), received);
            release.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertEquals(Arrays.asList(1, 2), received);
            assertFalse(interrupted.get());
            assertTrue(errors.isEmpty(), errors.toString());
        } finally {
            release.countDown();
            subscription.unsubscribe();
        }
    }

    @Test
    void timerRacesPreserveEveryItemAndSerializeCallbacks() throws Exception {
        PublishSubject<Integer> source = PublishSubject.create();
        List<Integer> received = new ArrayList<>();
        AtomicInteger inCallback = new AtomicInteger();
        AtomicBoolean concurrent = new AtomicBoolean();
        TestSubscriber<List<Integer>> result = new TestSubscriber<>();
        source.compose(new ConnectionBuffer<Integer>(ACTIVITY, 1, Schedulers.computation()))
                .doOnNext(batch -> {
                    if (inCallback.incrementAndGet() != 1) { concurrent.set(true); }
                    received.addAll(batch);
                    Thread.yield();
                    inCallback.decrementAndGet();
                }).subscribe(result);
        for (int i = 0; i < 20000; i++) {
            source.onNext(i);
            if (i % 100 == 0) { Thread.yield(); }
        }
        source.onCompleted();
        result.awaitTerminalEvent(5, TimeUnit.SECONDS);
        result.assertCompleted();
        result.assertNoErrors();
        assertFalse(concurrent.get());
        assertEquals(20000, received.size());
        for (int i = 0; i < received.size(); i++) { assertEquals(i, received.get(i).intValue()); }
    }

    @Test
    void configRejectsInvalidSettings() {
        assertThrows(IllegalArgumentException.class,
                () -> new ServerConfig.Builder<>().connectionBufferIntervalMs(0));
        assertThrows(IllegalArgumentException.class,
                () -> new ServerConfig.Builder<>().connectionBufferIntervalMs(-1));
        assertThrows(NullPointerException.class,
                () -> new ServerConfig.Builder<>().connectionBufferMode(null));
        assertThrows(IllegalArgumentException.class, () -> ServerConfig.ConnectionBufferMode.fromString("adaptive"));
    }
}
