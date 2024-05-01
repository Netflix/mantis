/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.control.examples;


import io.mantisrx.control.actuators.MantisJobActuator;
import io.mantisrx.control.controllers.ErrorComputer;
import io.mantisrx.control.controllers.Integrator;
import io.mantisrx.control.controllers.PIDController;
import rx.Observable;

/**
 * Simple example controller shows how one can create a PID which can auto-scale
 * a process on a single metric.
 */
public class ExampleAutoScaler implements Observable.Transformer<Double, Double> {

    private Double setPoint = 65.0;
    private Double rope = 5.0;

    private double kp = 0.01;
    private double ki = 0.01;
    private double kd = 0.01;

    private double min = 1.0;
    private double max = 10.0;

    @Override
    public Observable<Double> call(Observable<Double> cpuMeasurements) {

        return cpuMeasurements
                /*
                    The error computer here is going to take our stream of CPU
                    readings and compute an error value for our controller.

                    In this case we're targeting 65.0% CPU usage, and the problem
                    is inverted (scaling up causes CPU to decrease). Finally we
                    use a region of practical equivalence (ROPE) sometimes referred
                    to as a dead zone around our target of 65.0. The reason for this
                    is that it is difficult to hit 65% CPU usage exactly. This treats
                    the interval [60.0, 70.0] as the setPoint.
                 */
                .lift(new ErrorComputer(this.setPoint, true, rope))
                /*
                    The controller takes the error measurements and attempts
                    to determine the correct scale to track the setPoint. Each of the
                    gain parameters can be thought of as a factor multiplied by that
                    particular component.

                    kp: Proportional gain, multiplied by the error.
                    ki: Integral gain, multiplied by the sum of all errors. (Recall
                        error can be negative!)
                    kd: Derivative gain, multiplied by the diference between error now
                        and at t-1.
                 */
                .lift(new PIDController(kp, ki, kd))
                /*
                    The integrator's job is to sum up the output of the controller. The
                    reason we integrate is because our actuator expects whole size numbers. If
                    instead we were required to produce only the differences (+2 for scale up two
                    workers, -3 to scale down three then we would omit the integrator.
                 */
                .lift(new Integrator(1.0, min, max, 1.0))
                /*
                    We now feed this to an actuator which hows how to actually perform
                    the scaling action against the target. This can be achieved by performing a
                    REST call or communicating with other in-process systems.
                 */
                .lift(new MantisJobActuator("myJob", 1, "prod", "us-east-1", "main"))
                /*
                    Finally we perform some logging.
                 */
                .doOnNext(System.out::println);
    }
}
