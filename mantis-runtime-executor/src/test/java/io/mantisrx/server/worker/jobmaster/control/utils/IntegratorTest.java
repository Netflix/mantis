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

package io.mantisrx.server.worker.jobmaster.control.utils;

import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;


public class IntegratorTest {

    private final Observable<Double> data = Observable.just(1.0, -1.0, 0.0, -10.0);

    @Test
    public void shouldIntegrateOverInput() {

        Observable<Double> result = data.lift(new Integrator(0));

        TestSubscriber<Double> testSubscriber = new TestSubscriber<>();
        result.subscribe(testSubscriber);
        testSubscriber.assertCompleted();
        testSubscriber.assertValues(1.0, 0.0, 0.0, -10.0);
    }

    @Test
    public void shouldRespectMinimumValue() {

        Observable<Double> result = data.lift(new Integrator(0, 0.0, 10.0));
        TestSubscriber<Double> testSubscriber = new TestSubscriber<>();
        result.subscribe(testSubscriber);
        testSubscriber.assertCompleted();
        testSubscriber.assertValues(1.0, 0.0, 0.0, 0.0);
    }

    @Test
    public void shouldRespectMaximumValue() {

        Observable<Double> result = data.lift(new Integrator(0, -100.0, 0.0));
        TestSubscriber<Double> testSubscriber = new TestSubscriber<>();
        result.subscribe(testSubscriber);
        testSubscriber.assertCompleted();
        testSubscriber.assertValues(0.0, -1.0, -1.0, -11.0);
    }

    @Test
    public void shouldBeginFromInitialSuppliedValue() {
        Observable<Double> result = data.lift(new Integrator(1.0));

        TestSubscriber<Double> testSubscriber = new TestSubscriber<>();
        result.subscribe(testSubscriber);
        testSubscriber.assertCompleted();
        testSubscriber.assertValues(2.0, 1.0, 1.0, -9.0);
    }


}
