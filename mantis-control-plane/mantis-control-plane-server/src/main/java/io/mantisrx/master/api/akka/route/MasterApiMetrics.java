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

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;

public class MasterApiMetrics {
    private final Counter resp2xx;
    private final Counter resp4xx;
    private final Counter resp5xx;
    private final Counter incomingRequestCount;
    private final Counter throttledRequestCount;
    private final Counter askTimeOutCount;

    private static final MasterApiMetrics INSTANCE = new MasterApiMetrics();

    private MasterApiMetrics() {
        Metrics m = new Metrics.Builder()
                .id("MasterApiMetrics")
                .addCounter("incomingRequestCount")
                .addCounter("throttledRequestCount")
                .addCounter("resp2xx")
                .addCounter("resp4xx")
                .addCounter("resp5xx")
                .addCounter("askTimeOutCount")
                .build();
        Metrics metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.askTimeOutCount = metrics.getCounter("askTimeOutCount");
        this.resp2xx = metrics.getCounter("resp2xx");
        this.resp4xx = metrics.getCounter("resp4xx");
        this.resp5xx = metrics.getCounter("resp5xx");
        this.incomingRequestCount = metrics.getCounter("incomingRequestCount");
        this.throttledRequestCount = metrics.getCounter("throttledRequestCount");
    }

    public static final MasterApiMetrics getInstance() {
        return INSTANCE;
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

    public void incrementIncomingRequestCount() {
        incomingRequestCount.increment();
    }

    public void incrementThrottledRequestCount() {
        throttledRequestCount.increment();
    }
}
