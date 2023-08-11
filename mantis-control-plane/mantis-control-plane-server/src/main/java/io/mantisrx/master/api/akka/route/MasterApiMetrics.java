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

package io.mantisrx.master.api.akka.route;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;


public class MasterApiMetrics {
    private final Counter resp2xx;
    private final Counter resp4xx;
    private final Counter resp5xx;
    private final Counter askTimeOutCount;

    private  final MeterRegistry meterRegistry;
    public MasterApiMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.resp2xx = meterRegistry.counter("MasterApiMetrics_resp2xx");
        this.resp4xx = meterRegistry.counter("MasterApiMetrics_resp4xx");
        this.resp5xx = meterRegistry.counter("MasterApiMetrics_resp5xx");
        this.askTimeOutCount = meterRegistry.counter("MasterApiMetrics_askTimeOutCount");
    }


    public void incrementResp2xx() {
        resp2xx.increment();
    }

    public void incrementResp4xx() {
        resp4xx.increment();
    }

    public void incrementResp5xx() {
        resp5xx.increment();
    }

    public void incrementAskTimeOutCount() {
        askTimeOutCount.increment();
    }
}
