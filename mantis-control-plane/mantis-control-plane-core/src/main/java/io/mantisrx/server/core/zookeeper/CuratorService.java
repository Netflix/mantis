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

package io.mantisrx.server.core.zookeeper;

import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.core.Service;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.com.google.common.util.concurrent.MoreExecutors;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFrameworkFactory;
import io.mantisrx.shaded.org.apache.curator.framework.imps.GzipCompressionProvider;
import io.mantisrx.shaded.org.apache.curator.framework.state.ConnectionState;
import io.mantisrx.shaded.org.apache.curator.framework.state.ConnectionStateListener;
import io.mantisrx.shaded.org.apache.curator.retry.ExponentialBackoffRetry;
import lombok.extern.slf4j.Slf4j;


/**
 * This {@link Service} implementation is responsible for managing the lifecycle of a {@link io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework}
 * instance.
 */
@Slf4j
class CuratorService extends AbstractIdleService {
    private static final String isConnectedGaugeName = "isConnected";

    private final CuratorFramework curator;
    private final Gauge isConnectedGauge;

    public CuratorService(ZookeeperSettings settings) {
        Metrics m = new Metrics.Builder()
                .name(CuratorService.class.getCanonicalName())
                .addGauge(isConnectedGaugeName)
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        isConnectedGauge = m.getGauge(isConnectedGaugeName);

        curator = CuratorFrameworkFactory.builder()
                .compressionProvider(new GzipCompressionProvider())
                .connectionTimeoutMs((int) settings.getConnectionTimeout().toMillis())
                .retryPolicy(new ExponentialBackoffRetry((int) settings.getConnectionRetrySleepTime().toMillis(), settings.getConnectionRetryCount()))
                .connectString(settings.getConnectString())
                .build();
    }

    private void setupCuratorListener() {
        log.info("Setting up curator state change listener");
        curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState.isConnected()) {
                    log.info("Curator connected");
                    isConnectedGauge.set(1L);
                } else {
                    // ToDo: determine if it is safe to restart our service instead of committing suicide
                    log.error("Curator connection lost");
                    isConnectedGauge.set(0L);
                }
            }
        }, MoreExecutors.newDirectExecutorService());
    }

    @Override
    public void startUp() {
        try {
            isConnectedGauge.set(0L);
            setupCuratorListener();
            curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutDown() {
        try {
            curator.close();
        } catch (Exception e) {
            // A shutdown failure should not affect the subsequent shutdowns, so
            // we just warn here
            log.warn("Failed to shut down the curator service: " + e.getMessage(), e);
        }
    }

    public CuratorFramework getCurator() {
        return curator;
    }
}
