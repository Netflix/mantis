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


import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func0;


public class LeadershipManagerZkImpl implements ILeadershipManager {

    private static final Logger logger = LoggerFactory.getLogger(LeadershipManagerZkImpl.class);
    private final Gauge isLeaderGauge;
    private final AtomicLong isLeaderGaugeValue = new AtomicLong(0);
    private final Gauge isLeaderReadyGauge;
    private final AtomicLong isLeaderReadyGaugeValue = new AtomicLong(0);
    private final AtomicBoolean firstTimeLeaderMode = new AtomicBoolean(false);
    private final MasterConfiguration config;
    private final ServiceLifecycle serviceLifecycle;
    private volatile boolean isLeader = false;
    private volatile boolean isReady = false;
    private volatile Instant becameLeaderAt;
    private final MeterRegistry meterRegistry;

    public LeadershipManagerZkImpl(final MasterConfiguration config,
                                   final ServiceLifecycle serviceLifecycle,
                                   MeterRegistry meterRegistry) {
        this.config = config;
        this.serviceLifecycle = serviceLifecycle;
        this.meterRegistry = meterRegistry;
        Func0<Double> leaderSinceInSeconds = () -> {
            if (isLeader) {
                return (double) Duration.between(becameLeaderAt, Instant.now()).getSeconds();
            } else {
                return 0.0;
            }
        };

        isLeaderGauge = addGauge("isLeaderGauge", isLeaderGaugeValue);
        isLeaderReadyGauge = addGauge("isLeaderReadyGauge", isLeaderReadyGaugeValue);
        String groupName = MasterMain.class.getCanonicalName();
        Gauge.builder(groupName + "leaderSinceInSeconds", leaderSinceInSeconds::call)
            .register(meterRegistry);
    }

    private Gauge addGauge(String name, AtomicLong value) {
        String groupName = MasterMain.class.getCanonicalName();
        Gauge gauge = Gauge.builder(groupName + "_" + name, value::get)
            .register(meterRegistry);
        return gauge;
    }

    public void becomeLeader() {
        logger.info("Becoming leader now");
        if (firstTimeLeaderMode.compareAndSet(false, true)) {
            serviceLifecycle.becomeLeader();
            isLeaderGaugeValue.set(1L);
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
        isLeaderReadyGaugeValue.set(1L);
        isReady = true;
    }

    public void stopBeingLeader() {
        logger.info("Asked to stop being leader now");
        isReady = false;
        isLeader = false;
        isLeaderGaugeValue.set(0L);
        isLeaderReadyGaugeValue.set(0L);
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
