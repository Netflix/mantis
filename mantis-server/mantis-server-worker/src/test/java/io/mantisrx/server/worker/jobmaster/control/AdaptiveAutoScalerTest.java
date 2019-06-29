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

package io.mantisrx.server.worker.jobmaster.control;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.worker.jobmaster.JobAutoScaler;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;


public class AdaptiveAutoScalerTest {

    public static final String cfg = "{\"setPoint\": 10,\n" +
            " \"invert\": true,\n" +
            " \"rope\": 0.0,\n" +
            " \"kp\": 0.2,\n" +
            " \"ki\": 0.0,\n" +
            " \"kd\": 0.0,\n" +
            " \"minScale\": 2,\n" +
            " \"maxScale\": 5\n" +
            " }";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static long determineScale(double rps, double setPoint) {
        return Math.round(Math.ceil(rps / setPoint));
    }

    @Test
    public void shouldScaleUpUnderIncreasingLoadAndRespectMaximum() throws IOException {
        // Arrange
        Observable<Double> totalRPS = Observable.just(10.0, 20.0, 30.0, 40.0, 50.0, 60.0);
        AdaptiveAutoscalerConfig config = objectMapper.readValue(cfg, new TypeReference<AdaptiveAutoscalerConfig>() {});
        JobAutoScaler.StageScaler scaler = mock(JobAutoScaler.StageScaler.class);
        AdaptiveAutoscaler autoScaler = new AdaptiveAutoscaler(config, scaler, 2);
        TestSubscriber<Long> testSubscriber = new TestSubscriber<>();

        // Act
        AtomicLong numWorkers = new AtomicLong(2);
        totalRPS.map(rps -> new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.CPU,
                1,
                rps / (1.0 * numWorkers.get()),
                ((Long) numWorkers.get()).intValue(),
                "message"))
                .compose(autoScaler)
                .map(x -> (Long) x)
                .doOnNext(numWorkers::set)
                .subscribe(testSubscriber);

        // Assert

        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValues(2L, 2L, 3L, 4L, 5L, 5L);
    }

    @Test
    public void shouldScaleDownUnderDecreasingLoadAndRespectMinimum() throws IOException {
        // Arrange

        Observable<Double> totalRPS = Observable.just(60.0, 50.0, 40.0, 30.0, 20.0, 10.0, 10.0);
        AdaptiveAutoscalerConfig config = objectMapper.readValue(cfg, new TypeReference<AdaptiveAutoscalerConfig>() {});
        JobAutoScaler.StageScaler scaler = mock(JobAutoScaler.StageScaler.class);
        AdaptiveAutoscaler autoScaler = new AdaptiveAutoscaler(config, scaler, 5);
        TestSubscriber<Long> testSubscriber = new TestSubscriber<>();

        // Act
        AtomicLong numWorkers = new AtomicLong(5);
        totalRPS.map(rps -> new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.CPU,
                1,
                rps / (1.0 * numWorkers.get()),
                ((Long) numWorkers.get()).intValue(),
                "message"))
                .compose(autoScaler)
                .map(x -> (Long) x)
                .doOnNext(numWorkers::set)
                .subscribe(testSubscriber);

        // Assert

        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValues(5L, 5L, 5L, 4L, 3L, 2L, 2L);
    }

    @Test
    public void shouldPauseScalingWhenWaitingOnWorkersAndResumeAfter() throws IOException {
        // Arrange
        Observable<Double> totalRPS = Observable.just(10.0, 20.0, 30.0, 40.0, 50.0, 60.0);
        AdaptiveAutoscalerConfig config = objectMapper.readValue(cfg, new TypeReference<AdaptiveAutoscalerConfig>() {});
        JobAutoScaler.StageScaler scaler = mock(JobAutoScaler.StageScaler.class);
        AdaptiveAutoscaler autoScaler = new AdaptiveAutoscaler(config, scaler, 2);
        TestSubscriber<Long> testSubscriber = new TestSubscriber<>();

        // Act
        // This elaborate scheme skips the scaling action at the 40.0 RPS point
        // as we have not received the instances requested at the 30.0 RPS point
        AtomicLong numWorkers = new AtomicLong(2);
        AtomicLong count = new AtomicLong(0);
        totalRPS
                .doOnNext(x -> {
                    if (count.incrementAndGet() == 5) {
                        numWorkers.set(3);
                    }
                })
                .map(rps -> new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.CPU,
                        1,
                        rps / (1.0 * numWorkers.get()),
                        ((Long) numWorkers.get()).intValue(),
                        "message"))

                .compose(autoScaler)
                .map(x -> (Long) x)
                .doOnNext(n -> {
                    if (count.get() > 4) {
                        numWorkers.set(n);
                    }
                })
                .subscribe(testSubscriber);

        // Assert

        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValues(2L, 2L, 3L, 5L, 5L);
    }

    @Test
    public void shouldRemainConstantWhenValuesAreWithinRope() throws IOException {
        // Arrange
        final String cfg2 = "{\"setPoint\": 10,\n" +
                " \"invert\": true,\n" +
                " \"rope\": 3.0,\n" +
                " \"kp\": 0.2,\n" +
                " \"ki\": 0.0,\n" +
                " \"kd\": 0.0,\n" +
                " \"minScale\": 2,\n" +
                " \"maxScale\": 5\n" +
                " }";
        Observable<Double> totalRPS = Observable.just(30.0, 32.0, 28.0, 31.0, 30.0, 29.0, 31.0);
        AdaptiveAutoscalerConfig config = objectMapper.readValue(cfg2, new TypeReference<AdaptiveAutoscalerConfig>() {});
        JobAutoScaler.StageScaler scaler = mock(JobAutoScaler.StageScaler.class);
        AdaptiveAutoscaler autoScaler = new AdaptiveAutoscaler(config, scaler, 3);
        TestSubscriber<Long> testSubscriber = new TestSubscriber<>();

        // Act
        AtomicLong numWorkers = new AtomicLong(3);
        totalRPS.map(rps -> new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.CPU,
                1,
                rps / (1.0 * numWorkers.get()),
                ((Long) numWorkers.get()).intValue(),
                "message"))
                .compose(autoScaler)
                .map(x -> (Long) x)
                .doOnNext(numWorkers::set)
                .subscribe(testSubscriber);

        // Assert

        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValues(3L, 3L, 3L, 3L, 3L, 3L, 3L);
    }

}
