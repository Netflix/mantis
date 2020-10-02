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

package io.mantisrx.server.core.metrics;

import java.util.HashMap;
import java.util.Map;

import io.mantisrx.common.metrics.MetricsPublisher;
import io.mantisrx.server.core.Service;


public class MetricsPublisherService implements Service {

    private MetricsPublisher publisher;
    private int publishFrequency;
    private Map<String, String> commonTags = new HashMap<>();

    public MetricsPublisherService(MetricsPublisher publisher, int publishFrequency,
                                   Map<String, String> commonTags) {
        this.publisher = publisher;
        this.publishFrequency = publishFrequency;
        this.commonTags.putAll(commonTags);
    }

    public MetricsPublisherService(MetricsPublisher publisher, int publishFrequency) {
        this(publisher, publishFrequency, new HashMap<String, String>());
    }

    @Override
    public void start() {
        publisher.start(publishFrequency, commonTags);
    }

    @Override
    public void shutdown() {
        publisher.shutdown();
    }

    @Override
    public void enterActiveMode() {}

}
