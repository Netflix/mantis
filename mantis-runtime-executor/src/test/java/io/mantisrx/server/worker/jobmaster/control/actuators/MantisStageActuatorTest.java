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

package io.mantisrx.server.worker.jobmaster.control.actuators;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.mantisrx.server.worker.jobmaster.JobAutoScaler;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;


public class MantisStageActuatorTest {

    Observable<Double> data = Observable.just(1.1, 3.0, 2.85, 0.1);

    @Test
    public void shouldEchoCeilingOfInput() {
        JobAutoScaler.StageScaler mockScaler = mock(JobAutoScaler.StageScaler.class);
        when(mockScaler.scaleDownStage(any(Integer.class), any(Integer.class), any())).thenReturn(true);
        Observable<Double> result = data.lift(new MantisStageActuator(1, mockScaler));

        TestSubscriber<Double> testSubscriber = new TestSubscriber<>();
        result.subscribe(testSubscriber);
        testSubscriber.assertCompleted();
        testSubscriber.assertValues(2.0, 3.0, 3.0, 1.0);
    }

    @Test
    public void shouldCallScalerWhenInputChanged() {
        JobAutoScaler.StageScaler mockScaler = mock(JobAutoScaler.StageScaler.class);
        when(mockScaler.scaleDownStage(any(Integer.class), any(Integer.class), any())).thenReturn(true);
        Observable<Double> result = data.lift(new MantisStageActuator(1, mockScaler));

        TestSubscriber<Double> testSubscriber = new TestSubscriber<>();
        result.subscribe(testSubscriber);
        testSubscriber.assertCompleted();
        testSubscriber.assertValues(2.0, 3.0, 3.0, 1.0);

        verify(mockScaler).scaleUpStage(eq(1), eq(2), any());
        verify(mockScaler).scaleUpStage(eq(2), eq(3), any());
        verify(mockScaler).scaleDownStage(eq(3), eq(1), any());
    }

    @Test
    public void shouldReturnOriginalValueWhenScaleDownFails() {
        JobAutoScaler.StageScaler mockScaler = mock(JobAutoScaler.StageScaler.class);
        when(mockScaler.scaleDownStage(eq(3), eq(1), any())).thenReturn(false);

        MantisStageActuator actuator = new MantisStageActuator(3, mockScaler);
        Observable<Double> result = Observable.just(0.1).lift(actuator);

        TestSubscriber<Double> testSubscriber = new TestSubscriber<>();
        result.subscribe(testSubscriber);
        testSubscriber.assertCompleted();
        testSubscriber.assertValues(3.0); // Expecting the original value since scaleDownStage fails
    }
}
