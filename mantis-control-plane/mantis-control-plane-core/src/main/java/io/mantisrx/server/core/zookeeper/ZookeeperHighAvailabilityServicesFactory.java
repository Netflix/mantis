/*
 * Copyright 2022 Netflix, Inc.
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

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.server.core.config.MantisExtension;
import io.mantisrx.server.core.highavailability.HighAvailabilityServices;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import java.io.IOException;
import lombok.Getter;

public class ZookeeperHighAvailabilityServicesFactory implements MantisExtension<HighAvailabilityServices> {
    private final Config defaultZookeeperConfig =
        ConfigFactory
            .load()
            .getConfig("mantis.highAvailability.zookeeper");

    @Override
    public HighAvailabilityServices createObject(Config config, ActorSystem actorSystem) {
        final Config zookeeperConfig =
            config
                .getConfig("mantis.highAvailability.zookeeper")
                .withFallback(defaultZookeeperConfig);
        ZookeeperSettings settings = ZookeeperSettings.fromConfig(zookeeperConfig);
        return new ZkHighAvailabilityServices(settings);
    }

    private static class ZkHighAvailabilityServices extends AbstractIdleService implements HighAvailabilityServices {
        private final CuratorService curatorService;
        @Getter
        private final ZookeeperLeaderElectorService leaderElectorService;
        @Getter
        private final ZookeeperLeaderRetrievalService leaderRetrievalService;

        public ZkHighAvailabilityServices(ZookeeperSettings settings) {
            this.curatorService = new CuratorService(settings);
            this.leaderRetrievalService =
                new ZookeeperLeaderRetrievalService(curatorService.getCurator(), settings);
            this.leaderElectorService =
                new ZookeeperLeaderElectorService(curatorService.getCurator(), settings);
        }

        @Override
        protected void startUp() throws Exception {
            curatorService.startAsync().awaitRunning();
        }

        @Override
        public void shutDown() throws IOException {
            curatorService.stopAsync().awaitTerminated();
        }
    }
}
