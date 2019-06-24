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

package com.netflix.control.clutch;

import java.util.concurrent.TimeUnit;

import io.vavr.Tuple2;
import lombok.Builder;
import lombok.Value;

public @Builder @Value class ClutchConfiguration {

    public final Clutch.Metric metric;
    public final double setPoint;

    public final double kp;
    public final double ki;
    public final double kd;

    public final int minSize;
    public final int maxSize;

    public final Tuple2<Double, Double> rope;

    public final long cooldownInterval;
    public final TimeUnit cooldownUnits;
}
