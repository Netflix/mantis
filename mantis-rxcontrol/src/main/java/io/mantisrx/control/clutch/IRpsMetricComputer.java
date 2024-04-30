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

package io.mantisrx.control.clutch;

import io.vavr.Function2;
import java.util.Map;

/**
 * A function for computing the RPS metric to be compared against the setPoint and feed to the PID controller.
 * Arguments:
 * 1.) the clutch configuration for the current control loop
 * 2.) a Map containing metrics for computation
 * Return:
 * the computed RPS metric
 */
@FunctionalInterface
public interface IRpsMetricComputer extends Function2<ClutchConfiguration, Map<Clutch.Metric, Double>, Double> {
}
