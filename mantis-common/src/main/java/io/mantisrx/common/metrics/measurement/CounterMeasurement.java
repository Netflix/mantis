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

package io.mantisrx.common.metrics.measurement;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;


public class CounterMeasurement {

    private String event;
    private double count;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public CounterMeasurement(@JsonProperty("event") String event,
                              @JsonProperty("count") double count) {
        this.event = event;
        this.count = count;
    }

    public String getEvent() {
        return event;
    }

    public double getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "CounterMeasurement{" +
                "event='" + event + '\'' +
                ", count=" + count +
                '}';
    }
}
