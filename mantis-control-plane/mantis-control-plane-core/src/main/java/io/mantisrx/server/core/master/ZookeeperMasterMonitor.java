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

package io.mantisrx.server.core.master;

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.api.BackgroundCallback;
import io.mantisrx.shaded.org.apache.curator.framework.api.CuratorEvent;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.cache.NodeCache;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.cache.NodeCacheListener;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;


/**
 * A monitor that monitors the status of Mantis masters.
 */
public class ZookeeperMasterMonitor implements MasterMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMasterMonitor.class);

    private final CuratorFramework curator;
    private final String masterPath;
    private final BehaviorSubject<MasterDescription> masterSubject;
    private final AtomicReference<MasterDescription> latestMaster = new AtomicReference<>();
    private final NodeCache nodeMonitor;
    private final JsonSerializer jsonSerializer;

    public ZookeeperMasterMonitor(CuratorFramework curator, String masterPath) {
        this.curator = curator;
        this.masterPath = masterPath;
        this.masterSubject = BehaviorSubject.create();
        this.nodeMonitor = new NodeCache(curator, masterPath);
        this.jsonSerializer = new JsonSerializer();
    }

    public void start() throws Exception {
        nodeMonitor.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                retrieveMaster();
            }
        });

        try {
            nodeMonitor.start(true);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start master node monitor: " + e.getMessage(), e);
        }

        byte[] initialValue = nodeMonitor.getCurrentData().getData();
        onMasterNodeUpdated(initialValue);
        logger.info("The ZK master monitor has started");
    }

    private void onMasterNodeUpdated(byte[] data) throws Exception {
        logger.info("value was {}", new String(data));
        MasterDescription description = DefaultObjectMapper.getInstance().readValue(data, MasterDescription.class);
        logger.info("new master description = {}", description);
        latestMaster.set(description);
        masterSubject.onNext(description);
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
            logger.error("Failed to retrieve updated master information: " + e.getMessage(), e);
        }

    }

    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return masterSubject;
    }

    @Override
    public MasterDescription getLatestMaster() {
        return latestMaster.get();
    }

    public void shutdown() {
        try {
            nodeMonitor.close();
            logger.info("ZK master monitor is shut down");
        } catch (IOException e) {
            throw new RuntimeException("Failed to close the ZK node monitor: " + e.getMessage(), e);
        }
    }
}
