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

public class Integrator extends IController {

    private double sum = 0;
    private double min = Double.NEGATIVE_INFINITY;
    private double max = Double.POSITIVE_INFINITY;
    private double decayFactor = 1.0;

    public Integrator() {
    }

    public Integrator(double init) {
        this.sum = init;
    }

    public Integrator(double init, double min, double max) {
        this(init, min, max, 1.0);
    }

    public Integrator(double init, double min, double max, double decayFactor) {
        this.sum = init;
        this.min = min;
        this.max = max;
        this.decayFactor = decayFactor;
    }

    /**
     * A Clutch specific optimization, I don't like this one bit,
     * and would like to clean it up before OSS probably tearing down the
     * Rx pipeline and rewiring it instead.
     * @param val The value to which this integrator will be set.
     */
    public void setSum(double val) {
        this.sum = val;
    }

    @Override
    protected Double processStep(Double input) {
        double newSum = this.sum + input;
        newSum = (newSum > max) ? max : newSum;
        newSum = (newSum < min) ? min : newSum;
        this.sum = decayFactor * newSum;
        return newSum;
    }
}
