/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.master.domain;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Value;

/**
 * Represents the cost of running a job. Currently, this only tracks the daily cost of running a
 * job. But in the future, we may want to track other costs such as the cost of running a job for an
 * hour.
 * <p>
 * Similarly, we can also break up the cost into different components such as the cost of CPUs and
 * the cost of memory.
 * </p>
 */
@Value
public class Costs {

    Double dailyCost;

    @JsonIgnore
    public static final Costs ZERO = new Costs(0.0);

    public Costs multipliedBy(double multiplier) {
        return new Costs(dailyCost * multiplier);
    }

    public Costs plus(Costs other) {
        return new Costs(dailyCost + other.dailyCost);
    }
}
