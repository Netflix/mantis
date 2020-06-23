/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.iceberg.sink.committer.metrics;

import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;

public class CommitterMetrics {
    public CommitterMetrics() {
        Metrics metrics = new Metrics.Builder()
                .name(CommitterMetrics.class.getCanonicalName())
                // TODO: Add metrics
                .build();
        metrics = MetricsRegistry.getInstance().registerAndGet(metrics);

        // TODO: Get metrics, e.g., getCounter(name).
    }
}
