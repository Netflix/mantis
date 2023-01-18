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

import io.mantisrx.server.core.highavailability.LeaderRetrievalService;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.api.BackgroundCallback;
import io.mantisrx.shaded.org.apache.curator.framework.api.CuratorEvent;
import io.mantisrx.shaded.org.apache.curator.framework.listen.Listenable;
import io.mantisrx.shaded.org.apache.curator.framework.listen.ListenerContainer;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.cache.NodeCache;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.cache.NodeCacheListener;
import io.mantisrx.shaded.org.apache.curator.utils.ZKPaths;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * A monitor that monitors the status of Mantis masters.
 */
@Slf4j
public class ZookeeperLeaderRetrievalService extends AbstractIdleService implements LeaderRetrievalService {

    private final CuratorFramework curator;
    private final String masterPath;
    private final ListenerContainer<LeaderRetrievalService.Listener> listenable = new ListenerContainer<>();
    private final NodeCache nodeMonitor;

    public ZookeeperLeaderRetrievalService(CuratorFramework curator, ZookeeperSettings settings) {
        this(curator, ZKPaths.makePath(settings.getRootPath(), settings.getLeaderAnnouncementPath()));
    }

    public ZookeeperLeaderRetrievalService(CuratorFramework curator, String masterPath) {
        this.curator = curator;
        this.masterPath = masterPath;
        this.nodeMonitor = new NodeCache(curator, masterPath);
    }

    @Override
    public Listenable<LeaderRetrievalService.Listener> getListenable() {
        return listenable;
    }

    @Override
    public void startUp() throws Exception {
        nodeMonitor.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                retrieveMaster();
            }
        });

        nodeMonitor.start(true);
        log.info("The ZK master monitor has started");
    }

    private void onMasterNodeUpdated(@Nullable byte[] data) throws Exception {
        if (data != null) {
            log.info("value is {}", new String(data));
        }

        listenable.forEach(listener -> {
            try {
                listener.onLeaderChanged(data);
            } catch (Exception e) {
                log.error("Failed to update listener {} on leader changed to {}", listener, data != null ? new String(data) : "null", e);
            }
            return null;
        });
    }

    private void retrieveMaster() {
        try {
            curator
                .sync()  // sync with ZK before reading
                .inBackground(
                    curator
                        .getData()
                        .inBackground(new BackgroundCallback() {
                            @Override
                            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                                onMasterNodeUpdated(event.getData());
                            }
                        })
                        .forPath(masterPath)
                )
                .forPath(masterPath);

        } catch (Exception e) {
            log.error("Failed to retrieve updated master information: " + e.getMessage(), e);
        }

    }

    @Override
    public void shutDown() throws Exception {
        nodeMonitor.close();
        log.info("ZK master monitor is shut down");
    }
}
