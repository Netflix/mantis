/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.server.core.master;

import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.ILeaderElectorFactory;
import io.mantisrx.server.core.ILeaderMonitorFactory;
import io.mantisrx.server.core.ILeadershipManager;

/**
 * Local leader manager factory. This design is intended for non-production use only. The
 * local leader is the single node operating as the leader.
 */
public class LocalLeaderFactory implements ILeaderMonitorFactory, ILeaderElectorFactory {
    @Override
    public BaseService createLeaderElector(CoreConfiguration config, ILeadershipManager manager) {
        return new BaseService() {
            @Override
            public void start() {
                manager.becomeLeader();
            }
        };
    }

    @Override
    public MasterMonitor createLeaderMonitor(CoreConfiguration config) {

        String[] parts = config.getZkConnectionString().split(":");
        if (parts.length != 2) {
            throw new RuntimeException(
                "invalid local mode connection string: " + config.getZkConnectionString());
        }

        int apiPort = Integer.parseInt(parts[1]);
        final MasterDescription md = new MasterDescription(
            parts[0],
            "127.0.0.1",
            apiPort,
            apiPort,
            apiPort,
            "api/postjobstatus",
            apiPort + 6,
            System.currentTimeMillis());
        return new LocalMasterMonitor(md);
    }
}
