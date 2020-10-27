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

package io.mantisrx.common.metrics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;


public abstract class MetricsPublisher {

    protected Map<String, String> commonTags = new HashMap<>();
    private Properties properties;
    private Subscription subscription;

    public MetricsPublisher(Properties properties) {
        this.properties = properties;
    }

    protected Properties getPropertis() {
        return properties;
    }

    public void start(int pollMetricsRegistryFrequencyInSeconds, final Map<String, String> commonTags) {
        this.commonTags.putAll(commonTags);
        // read from metrics registry every publishRateInSeconds
        final MetricsRegistry registry = MetricsRegistry.getInstance();
        subscription =
                Observable.interval(0, pollMetricsRegistryFrequencyInSeconds, TimeUnit.SECONDS)
                        .doOnNext(new Action1<Long>() {
                            @Override
                            public void call(Long t1) {
                                final long timestamp = System.currentTimeMillis();
                                publishMetrics(timestamp, registry.metrics());
                            }
                        }).subscribe();
    }

    public void start(int pollMetricsRegistryFrequencyInSeconds) {
        start(pollMetricsRegistryFrequencyInSeconds, new HashMap<String, String>());
    }

    public void shutdown() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }

    public abstract void publishMetrics(long timestamp, Collection<Metrics> currentMetricsRegistered);
}
