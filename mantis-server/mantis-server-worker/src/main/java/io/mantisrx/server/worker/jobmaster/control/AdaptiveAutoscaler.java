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

import io.mantisrx.server.worker.jobmaster.JobAutoScaler;
import io.mantisrx.server.worker.jobmaster.control.actuators.MantisStageActuator;
import io.mantisrx.server.worker.jobmaster.control.controllers.PIDController;
import io.mantisrx.server.worker.jobmaster.control.utils.ErrorComputer;
import io.mantisrx.server.worker.jobmaster.control.utils.Integrator;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


/**
 * AutoScaler is an Rx Transformer which can be composed with a stream
 * of Metrics in order to scale a specific stage.
 */
public class AdaptiveAutoscaler implements Observable.Transformer<JobAutoScaler.Event, Object> {

    private static final Logger logger = LoggerFactory.getLogger(AdaptiveAutoscaler.class);

    private final AdaptiveAutoscalerConfig config;
    private final JobAutoScaler.StageScaler scaler;
    private final long initialSize;
    private final AtomicLong targetScale = new AtomicLong(0);

    public AdaptiveAutoscaler(AdaptiveAutoscalerConfig config, JobAutoScaler.StageScaler scaler, int initialSize) {
        this.config = config;
        this.scaler = scaler;
        this.initialSize = initialSize;
        this.targetScale.set(initialSize);
    }

    @Override
    public Observable<Object> call(Observable<JobAutoScaler.Event> metrics) {

        return metrics
                .filter(metric -> ((long) metric.getNumWorkers()) == targetScale.get())
                .map(JobAutoScaler.Event::getValue)
                .lift(new ErrorComputer(config.setPoint, config.invert, config.rope))
                .lift(PIDController.of(config.kp, config.ki, config.kd))
                .lift(new Integrator(this.initialSize, config.minScale, config.maxScale))
                .lift(new MantisStageActuator(this.initialSize, scaler))
                .map(Math::round)
                .doOnNext(targetScale::set)
                .map(x -> x); // TODO: Necessary?
    }
}
