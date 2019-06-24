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

package io.mantisrx.runtime.source.http.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action1;


public class OperatorResumeOnErrorTest {

    @Test
    public void testCanResumeOnError() throws Exception {
        final int threshold = 6;
        final Observable<Integer> ints = createIntegerStreamThatFailsOnThresholdValue(threshold);

        final int repeat = 5;
        final AtomicInteger retries = new AtomicInteger();
        Operator<Integer, Integer> resumeOperator = resumeWithFixNumberOfRetries(ints, repeat, retries);


        final CountDownLatch done = new CountDownLatch(1);
        final AtomicInteger errorCount = new AtomicInteger();
        final AtomicReference<Throwable> error = new AtomicReference<>();

        ints
                .lift(resumeOperator)
                .subscribe(new Subscriber<Integer>() {
                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        errorCount.incrementAndGet();
                        error.set(e);
                        done.countDown();
                    }

                    @Override
                    public void onNext(Integer integer) {
                        assertTrue("The integer should not be over the threshold. The integer value: " + integer, integer < threshold);
                    }
                });

        long timeoutSecs = 5;
        if (!done.await(5, TimeUnit.SECONDS)) {
            fail("Should finish within " + timeoutSecs + " seconds");
        }

        assertEquals(String.format("There should be exactly %d retries", repeat), repeat, retries.get());
        assertEquals("There should be exactly one onError", 1, errorCount.get());
        assertEquals("The error should be the user created one. ", TestException.class, error.get().getClass());
    }

    private OperatorResumeOnError<Integer> resumeWithFixNumberOfRetries(final Observable<Integer> ints, final int repeat, final AtomicInteger retries) {
        return new OperatorResumeOnError<>(
                new ResumeOnErrorPolicy<Integer>() {
                    @Override
                    public Observable<Integer> call(final Integer attempts, final Throwable error) {
                        if (attempts > repeat) {
                            return null;
                        }

                        retries.incrementAndGet();
                        return ints;
                    }
                });
    }

    private Observable<Integer> createIntegerStreamThatFailsOnThresholdValue(final int threshold) {
        return Observable.create(new OnSubscribe<Integer>() {
            @Override
            public void call(final Subscriber<? super Integer> subscriber) {
                Observable
                        .just(1, 2, 3, 4, 5, 6)
                        .doOnNext(new Action1<Integer>() {
                            @Override
                            public void call(Integer value) {
                                if (value == threshold) {
                                    subscriber.onError(new TestException("Failed on value " + value));
                                } else {
                                    subscriber.onNext(value);
                                }
                            }
                        }).subscribe();

            }
        });
    }

    public static class TestException extends RuntimeException {

        public TestException(String message) {
            super(message);
        }
    }
}
