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

package io.mantisrx.control.controllers;

import io.mantisrx.control.IController;

/**
 * The loss computation is generally the first step in a control system.
 * The responsibility of this component is to compute the loss function
 * on the output of the system under control.
 *
 * Example:
 * [ErrorComputer] -> PIDController -> Integrator -> Actuator
 *
 * The loss acts as input for the control system.
 */
public class ErrorComputer extends IController {

    private final double setPoint;
    private final boolean inverted;
    private final double lowerRope;
    private final double upperRope;

    /**
     *
     * @param setPoint The target value for the metric being tracked.
     * @param inverted A boolean indicating whether or not to invert output. Output is generally inverted if increasing
     *                 the plant input will decrease the output. For example when autoscaling increasing the number
     *                 of worker instances will decrease messages processed per instance. This is an inverted problem.
     * @param lowerRope Region of practical equivalence (ROPE) -- a region surrounding the setpoint considered equal to the setpoint.
     * @param upperRope Region of practical equivalence (ROPE) -- a region surrounding the setpoint considered equal to the setpoint.
     */
    public ErrorComputer(double setPoint, boolean inverted, double lowerRope, double upperRope) {
        this.setPoint = setPoint;
        this.inverted = inverted;
        this.lowerRope = lowerRope;
        this.upperRope = upperRope;
    }

    public ErrorComputer(double setPoint, boolean inverted, double rope) {
        this.setPoint = setPoint;
        this.inverted = inverted;
        this.lowerRope = rope;
        this.upperRope = rope;
    }

    @Override
    public Double processStep(Double input) {
        return inverted ?
                -1.0 * loss(setPoint, input, lowerRope, upperRope) :
                loss(setPoint, input, lowerRope, upperRope);
    }

    /**
     * Computes the correct loss value considering all values within [setPoint-rope, setPoint+rope] are considered
     * to be equivalent. Error must grow linearly once the value is outside of the ROPE, without a calculation such as
     * this the loss is a step function once crossing the threshold, with this function loss is zero and linearly
     * increases as it deviates from the setpoint and ROPE.
     *
     * @param setPoint The configured setPoint.
     * @param observed The observed metric to be compared to the setPoint.
     * @param lowerRope The region of practical equivalence (ROPE) on the lower end.
     * @param upperRope The region of practical equivalence (ROPE) on the upper end.
     * @return Error adjusted for the ROPE.
     */
    public static double loss(double setPoint, double observed, double lowerRope, double upperRope) {
        if (observed > setPoint + upperRope) {
            return (setPoint + upperRope) - observed;
        } else if (observed < setPoint - lowerRope) {
            return (setPoint - lowerRope) - observed;
        }
        return 0.0;
    }
}
