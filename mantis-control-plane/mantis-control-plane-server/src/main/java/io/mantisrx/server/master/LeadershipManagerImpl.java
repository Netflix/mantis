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

package io.mantisrx.server.master;

import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.GaugeCallback;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.config.MasterConfiguration;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func0;


public class LeadershipManagerImpl implements ILeadershipManager {

    private static final Logger logger = LoggerFactory.getLogger(LeadershipManagerImpl.class);
    private final Gauge isLeaderGauge;
    private final Gauge isLeaderReadyGauge;
    private final AtomicBoolean firstTimeLeaderMode = new AtomicBoolean(false);
    private final MasterConfiguration config;
    private final ServiceLifecycle serviceLifecycle;
    private volatile boolean isLeader = false;
    private volatile boolean isReady = false;
    private volatile Instant becameLeaderAt;

    public LeadershipManagerImpl(final MasterConfiguration config,
                                   final ServiceLifecycle serviceLifecycle) {
        this.config = config;
        this.serviceLifecycle = serviceLifecycle;
        MetricGroupId metricGroupId = new MetricGroupId(MasterMain.class.getCanonicalName());
        Func0<Double> leaderSinceInSeconds = () -> {
            if (isLeader) {
                return (double) Duration.between(becameLeaderAt, Instant.now()).getSeconds();
            } else {
                return 0.0;
            }
        };

        Metrics m = new Metrics.Builder()
                .id(metricGroupId)
                .addGauge("isLeaderGauge")
                .addGauge("isLeaderReadyGauge")
                .addGauge(new GaugeCallback(metricGroupId, "leaderSinceInSeconds", leaderSinceInSeconds))
                .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        isLeaderGauge = m.getGauge("isLeaderGauge");
        isLeaderReadyGauge = m.getGauge("isLeaderReadyGauge");
    }

    public void becomeLeader() {
        logger.info("Becoming leader now");
        if (firstTimeLeaderMode.compareAndSet(false, true)) {
            serviceLifecycle.becomeLeader();
            isLeaderGauge.set(1L);
            becameLeaderAt = Instant.now();
        } else {
            logger.warn("Unexpected to be told to enter leader mode more than once, ignoring.");
        }
        isLeader = true;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public boolean isReady() {
        return isReady;
    }

    public void setLeaderReady() {
        logger.info("marking leader READY");
        isLeaderReadyGauge.set(1L);
        isReady = true;
    }

    public void stopBeingLeader() {
        logger.info("Asked to stop being leader now");
        isReady = false;
        isLeader = false;
        isLeaderGauge.set(0L);
        isLeaderReadyGauge.set(0L);
        if (!firstTimeLeaderMode.get()) {
            logger.warn("Unexpected to be told to stop being leader when we haven't entered leader mode before, ignoring.");
            return;
        }
        // Various services may have built in-memory state that is currently not easy to revert to initialization state.
        // Until we create such a lifecycle feature for each service and all of their references, best thing to do is to
        //  exit the process and depend on a watcher process to restart us right away. Especially since restart isn't
        // very expensive.
        logger.error("Exiting due to losing leadership after running as leader");
        System.exit(1);
    }

    public MasterDescription getDescription() {

        return new MasterDescription(
                getHost(),
                getHostIP(),
                config.getApiPort(),
                config.getSchedInfoPort(),
                config.getApiPortV2(),
                config.getApiStatusUri(),
                config.getConsolePort(),
                System.currentTimeMillis()
        );
    }

    private String getHost() {
        String host = config.getMasterHost();
        if (host != null) {
            return host;
        }

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get the host information: " + e.getMessage(), e);
        }
    }

    private String getHostIP() {
        String ip = config.getMasterIP();
        if (ip != null) {
            return ip;
        }

        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get the host information: " + e.getMessage(), e);
        }
    }
}
