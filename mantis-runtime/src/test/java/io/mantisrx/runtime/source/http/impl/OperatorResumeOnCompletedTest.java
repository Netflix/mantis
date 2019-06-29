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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;


public class OperatorResumeOnCompletedTest {

    @Test
    public void testCanResumeOnCompletion() throws Exception {
        final int max = 6;
        final Observable<Integer> ints = Observable.range(1, max);

        final int repeat = 5;
        final AtomicInteger retries = new AtomicInteger();
        Operator<Integer, Integer> resumeOperator = new OperatorResumeOnCompleted<>(
                new ResumeOnCompletedPolicy<Integer>() {
                    @Override
                    public Observable<Integer> call(final Integer attempts) {
                        if (attempts > repeat) {
                            return null;
                        }
                        retries.incrementAndGet();
                        return Observable.just(attempts + max);

                    }
                });

        final CountDownLatch done = new CountDownLatch(1);
        final AtomicInteger completionCount = new AtomicInteger();
        final List<Integer> collected = new ArrayList<>();
        ints
                .lift(resumeOperator)
                .subscribe(new Subscriber<Integer>() {
                    @Override
                    public void onCompleted() {
                        completionCount.incrementAndGet();
                        done.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        fail("There should be no error at all");
                        done.countDown();
                    }

                    @Override
                    public void onNext(Integer integer) {
                        collected.add(integer);
                    }
                });

        long timeoutSecs = 5;
        if (!done.await(5, TimeUnit.SECONDS)) {
            fail("Should finish within " + timeoutSecs + " seconds");
        }

        assertEquals(String.format("There should be exactly %d retries", repeat), repeat, retries.get());
        assertEquals("There should be exactly one onCompleted call", 1, completionCount.get());
        List<Integer> expected = Observable.range(1, max + repeat).toList().toBlocking().first();
        assertEquals("The collected should include the original stream plus every attempt", expected, collected);
    }
}
