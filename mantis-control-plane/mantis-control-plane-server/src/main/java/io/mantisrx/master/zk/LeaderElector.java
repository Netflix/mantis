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

package io.mantisrx.master.zk;

import static io.mantisrx.shaded.org.apache.zookeeper.KeeperException.Code.OK;

import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.leader.LeaderLatch;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import io.mantisrx.shaded.org.apache.zookeeper.CreateMode;
import io.mantisrx.shaded.org.apache.zookeeper.data.Stat;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * see {@link ZookeeperLeadershipFactory} and {@link ZookeeperLeaderElector}
 */
@Deprecated
public class LeaderElector extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElector.class);

    private volatile boolean started = false;

    private final ObjectMapper jsonMapper;
    private final ILeadershipManager leadershipManager;
    private final LeaderLatch leaderLatch;
    private final CuratorFramework curator;
    private final String electionPath;
    // The path where a selected leader announces itself.
    private final String leaderPath;


    private LeaderElector(ObjectMapper jsonMapper,
                          ILeadershipManager leadershipManager,
                          CuratorFramework curator,
                          String electionPath,
                          String leaderPath) {
        super(false);
        this.jsonMapper = jsonMapper;
        this.leadershipManager = leadershipManager;
        this.curator = curator;
        this.leaderLatch = createNewLeaderLatch(electionPath);
        this.electionPath = electionPath;
        this.leaderPath = leaderPath;
    }

    @Override
    public void start() {
        if (started) {
            return;
        }
        started = true;

        try {
            Stat pathStat = curator.checkExists().forPath(leaderPath);
            // Create the path only if the path does not exist
            if(pathStat == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(leaderPath);
            }

            leaderLatch.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create a leader elector for master: "+e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        try {
            leaderLatch.close();
        } catch (IOException e) {
            logger.warn("Failed to close the leader latch: "+e.getMessage(), e);
        }finally {
            started = false;
        }
    }

    private LeaderLatch createNewLeaderLatch(String leaderPath) {
        final LeaderLatch newLeaderLatch = new LeaderLatch(curator, leaderPath, "127.0.0.1");

        newLeaderLatch.addListener(
            new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    announceLeader();
                }

                @Override
                public void notLeader() {
                    leadershipManager.stopBeingLeader();
                }
            }, Executors.newSingleThreadExecutor(new DefaultThreadFactory("MasterLeader-%s")));

        return newLeaderLatch;
    }

    private void announceLeader() {
        try {
            logger.info("Announcing leader");
            byte[] masterDescription = jsonMapper.writeValueAsBytes(leadershipManager.getDescription());

            // There is no need to lock anything because we ensure only leader will write to the leader path
            curator
                .setData()
                .inBackground((client, event) -> {
                    if (event.getResultCode() == OK.intValue()) {
                        leadershipManager.becomeLeader();
                    } else {
                        logger.warn("Failed to elect leader from path {} with event {}", leaderPath, event);
                    }
                }).forPath(leaderPath, masterDescription);
        } catch (Exception e) {
            throw new RuntimeException("Failed to announce leader: "+e.getMessage(), e);
        }

    }

    public static LeaderElector.Builder builder(ILeadershipManager manager) {
        return new LeaderElector.Builder(manager);
    }

    public static class Builder {
        private ObjectMapper jsonMapper;
        private ILeadershipManager leadershipManager;
        private CuratorFramework curator;
        private String electionPath;
        private String announcementPath;

        public Builder(ILeadershipManager leadershipManager){
            this.leadershipManager = leadershipManager;
        }

        public LeaderElector.Builder withJsonMapper(ObjectMapper jsonMapper) {
            this.jsonMapper = jsonMapper;
            return this;
        }

        public LeaderElector.Builder withCurator(CuratorFramework curator) {
            this.curator = curator;
            return this;
        }

        public LeaderElector.Builder withElectionPath(String path) {
            this.electionPath = path;
            return this;
        }

        public LeaderElector.Builder withAnnouncementPath(String annPath) {
            this.announcementPath = annPath;
            return this;
        }

        public LeaderElector build() {
            return new LeaderElector(jsonMapper, leadershipManager, curator, electionPath, announcementPath);
        }
    }
}
