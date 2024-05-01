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

package io.mantisrx.control;

import rx.functions.Func1;

/**
 * Same interface as IController but separated so that actuators can be identified.
 *
 * The job of the actuator is to receive a size and alter the size of the target
 * to be autoscaled. This might for example be a Mantis Job, a Flink Router or even
 * an AWS Autoscaling Group.
 */
public abstract class IActuator extends IController {

    /**
     * Static factory method for constructing an instance of IAcuator.
     *
     * @param fn A function which presumably side-effects for actuation. Should return its input.
     * @return An IActuator which calls fn with the value.
     */
    public static IActuator of(Func1<Double, Double> fn) {

       return new IActuator() {
           @Override
           protected Double processStep(Double value) {
               return fn.call(value);
           }
       };
    }
}
