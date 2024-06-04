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

package io.mantisrx.master.zk;

import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.ILeaderElectorFactory;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.ZookeeperLeaderMonitorFactory;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.server.master.config.ConfigurationProvider;
import io.mantisrx.server.master.config.MasterConfiguration;
import io.mantisrx.shaded.org.apache.curator.utils.ZKPaths;

public class ZookeeperLeadershipFactory extends ZookeeperLeaderMonitorFactory implements ILeaderElectorFactory {
    @Override
    public BaseService createLeaderElector(final CoreConfiguration config, final ILeadershipManager leadershipManager) {
        final MasterConfiguration mConfig = ConfigurationProvider.getConfig();
        final CuratorService cs = getCuratorService(config);
        // we are starting here because this was started when created in MasterMain
        // this keeps compatibility with the same startup
        cs.start();
        return new ZookeeperLeaderElector(
            DefaultObjectMapper.getInstance(),
            leadershipManager,
            cs,
            ZKPaths.makePath(config.getZkRoot(), mConfig.getLeaderElectionPath()),
            ZKPaths.makePath(config.getZkRoot(), config.getLeaderAnnouncementPath())
            );
    }
}
