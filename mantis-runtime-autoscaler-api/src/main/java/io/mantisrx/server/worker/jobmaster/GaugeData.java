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

package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.common.metrics.measurement.GaugeMeasurement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class GaugeData {

    private final long when;
    private Map<String, Double> gauges = new HashMap<>();

    GaugeData(final long when, final List<GaugeMeasurement> gauges) {
        this.when = when;
        for (GaugeMeasurement gauge : gauges) {
            this.gauges.put(gauge.getEvent(), (double) gauge.getValue());
        }
    }

    GaugeData(final long when, final Map<String, Double> gauges) {
        this.when = when;
        this.gauges = gauges;
    }

    public long getWhen() {
        return when;
    }

    public Map<String, Double> getGauges() {
        return gauges;
    }

    @Override
    public String toString() {
        return "GaugeData{" +
                "when=" + when +
                ", gauges=" + gauges +
                '}';
    }
}
