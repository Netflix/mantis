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

import static io.mantisrx.shaded.org.apache.zookeeper.KeeperException.Code.OK;

import io.mantisrx.server.core.highavailability.LeaderElectorService;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.leader.LeaderLatch;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import io.mantisrx.shaded.org.apache.curator.utils.ZKPaths;
import io.mantisrx.shaded.org.apache.zookeeper.CreateMode;
import io.mantisrx.shaded.org.apache.zookeeper.data.Stat;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ZookeeperLeaderElectorService extends AbstractIdleService implements LeaderElectorService {

//    private volatile boolean started = false;
//    private volatile boolean isLeader = false;

//    private final ListenerContainer<LeaderElectorService.Listener> listeners =
//        new ListenerContainer<>();

    //    private final ObjectMapper jsonMapper;
//    private final ILeadershipManager leadershipManager;
    private final AtomicReference<Contender> contenderRef = new AtomicReference<>();
    private final LeaderLatch leaderLatch;
    private final CuratorFramework curator;
    // The path where a selected leader announces itself.
    private final String leaderPath;


    ZookeeperLeaderElectorService(CuratorFramework curator, ZookeeperSettings settings) {
        this(
            curator,
            ZKPaths.makePath(settings.getRootPath(), settings.getLeaderElectionPath()),
            ZKPaths.makePath(settings.getRootPath(), settings.getLeaderAnnouncementPath()));
    }

    ZookeeperLeaderElectorService(
        CuratorFramework curator,
        String electionPath,
        String leaderPath) {
//        this.jsonMapper = jsonMapper;
//        this.leadershipManager = leadershipManager;
        this.curator = curator;
        this.leaderLatch = createNewLeaderLatch(electionPath);
        this.leaderPath = leaderPath;
    }

    @Override
    public void setContender(Contender contender) {
        if (!contenderRef.compareAndSet(null, contender)) {
            throw new RuntimeException("Only 1 contender was expected");
        }
    }

    @Override
    public Contender getContender() {
        Preconditions.checkState(contenderRef.get() != null, "Contender was not set");
        return contenderRef.get();
    }

    @Override
    public void startUp() {
        Preconditions.checkState(
            contenderRef.get() != null,
            "contender is expected to be set before the service is started");
//        if (started) {
//            return;
//        }
//        started = true;

        try {
            Stat pathStat = curator.checkExists().forPath(leaderPath);
            // Create the path only if the path does not exist
            if (pathStat == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(leaderPath);
            }

            leaderLatch.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create a leader elector for master: " + e.getMessage(), e);
        }
    }

    @Override
    public void shutDown() {
        try {
            leaderLatch.close();
        } catch (IOException e) {
            log.warn("Failed to close the leader latch: " + e.getMessage(), e);
        }
    }

//    @Override
//    public boolean hasLeadership() {
//        return isLeader;
//    }

//    @Override
//    public void setLeaderMetadata(byte[] data) throws IOException {
//        try {
//            Preconditions.checkState(isLeader, "too late");
//            curator.setData().forPath(leaderPath, data);
//        } catch (Exception e) {
//            throw new IOException(e);
//        }
//    }

//    @Override
//    public Listenable<LeaderElectorService.Listener> getListenable() {
//        return listeners;
//    }

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
                    stopBeingLeader();
                }
            }, Executors.newSingleThreadExecutor(new DefaultThreadFactory("MasterLeader-%s")));

        return newLeaderLatch;
    }

//    private void announceLeader() {
//        isLeader = true;
//        listeners.forEach(new Function<LeaderElectorService.Listener, Void>() {
//            @Nullable
//            @Override
//            public Void apply(@Nullable LeaderElectorService.Listener listener) {
//                try {
//                    Preconditions.checkNotNull(listener, "listener is null");
//                    listener.isLeader();
//                } catch (Exception e) {
//                    log.error("Calling listener failed", e);
//                }
//                return null;
//            }
//        });
//    }

    private void stopBeingLeader() {
        Preconditions.checkState(state().equals(State.RUNNING), "state=%s does not equal RUNNING", state());
        try {
            contenderRef.get().onNotLeader();
        } catch (Exception e) {
            log.error("Calling listener failed", e);
        }
    }

    private void announceLeader() {
        Preconditions.checkState(state().equals(State.RUNNING), "state=%s does not equal RUNNING", state());
        try {
            log.info("Announcing leader for {}", contenderRef.get());
//            byte[] masterDescription = jsonMapper.writeValueAsBytes(leadershipManager.getDescription());

            // There is no need to lock anything because we ensure only leader will write to the leader path
            curator
                .setData()
                .inBackground((client, event) -> {
                    if (event.getResultCode() == OK.intValue()) {
                        contenderRef.get().onLeader();
                    } else {
                        log.warn("Failed to elect leader from path {} with event {}", leaderPath, event);
                    }
                }).forPath(leaderPath, contenderRef.get().getContenderMetadata());
        } catch (Exception e) {
            throw new RuntimeException("Failed to announce leader: " + e.getMessage(), e);
        }
    }

//    public static ZookeeperLeaderElector.Builder builder(ILeadershipManager manager) {
//        return new ZookeeperLeaderElector.Builder(manager);
//    }
}
