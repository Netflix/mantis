/*
 * Copyright 2023 Netflix, Inc.
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

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.core.highavailability.LeaderElectorService;
import io.mantisrx.server.core.highavailability.LeaderElectorService.Contender;
import io.mantisrx.server.core.highavailability.NodeSettings;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LeaderElectorServiceContenderImpl implements Contender {
    private final Gauge isLeaderGauge;
    private final Gauge isLeaderReadyGauge;
    private volatile boolean isLeader;
    private final AtomicBoolean firstTimeLeaderMode = new AtomicBoolean(false);
    private final MasterDescription masterDescription;
    private final ServiceLifecycle serviceLifecycle;
    private final byte[] masterDescriptionBytes;

    public LeaderElectorServiceContenderImpl(
        ServiceLifecycle serviceLifecycle,
        NodeSettings settings,
        LeaderElectorService leaderElectorService) throws IOException {
        this.serviceLifecycle = serviceLifecycle;
        Metrics m = new Metrics.Builder()
            .name(LeaderElectorServiceContenderImpl.class.getCanonicalName())
            .addGauge("isLeaderGauge")
            .addGauge("isLeaderReadyGauge")
            .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        isLeaderGauge = m.getGauge("isLeaderGauge");
        isLeaderReadyGauge = m.getGauge("isLeaderReadyGauge");
        this.masterDescription = new MasterDescription(
            settings.getHost(),
            settings.getIp(),
            settings.getApiPort(),
            settings.getScheduleInfoPort(),
            settings.getApiPortV2(),
            settings.getApiStatusURI(),
            settings.getConsolePort(),
            System.currentTimeMillis());
        this.masterDescriptionBytes = new JsonSerializer().toJsonBytes(masterDescription);
        leaderElectorService.setContender(this);
    }

    @Override
    public void onLeader() {
        log.info("Becoming leader now");
        if (firstTimeLeaderMode.compareAndSet(false, true)) {
            try {
                isLeader = true;
                serviceLifecycle.becomeLeader();
                isLeaderGauge.set(1L);
            } catch (Exception e) {
                // we were not expecting this exception. Throwing this for now.
                throw new RuntimeException(e);
            }
        } else {
            log.warn("Unexpected to be told to enter leader mode more than once, ignoring.");
        }
    }

    @Override
    public void onNotLeader() {
        log.info("Asked to stop being leader now");
        isLeader = false;
        isLeaderGauge.set(0L);
        isLeaderReadyGauge.set(0L);
        if (!firstTimeLeaderMode.compareAndSet(true, false)) {
            log.warn("Unexpected to be told to stop being leader when we haven't entered leader mode before, ignoring.");
            return;
        }
        // Various services may have built in-memory state that is currently not easy to revert to initialization state.
        // Until we create such a lifecycle feature for each service and all of their references, best thing to do is to
        //  exit the process and depend on a watcher process to restart us right away. Especially since restart isn't
        // very expensive.
        log.error("Exiting due to losing leadership after running as leader");
        System.exit(1);
    }

    @Override
    public byte[] getContenderMetadata() {
        return masterDescriptionBytes;
    }

    @Override
    public Executor getExecutor() {
        return MoreExecutors.directExecutor();
    }

    @Override
    public boolean hasLeadership() {
        return isLeader;
    }

    public MasterDescription getDescription() {
        return this.masterDescription;
    }
}
