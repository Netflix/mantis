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

import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.api.BackgroundCallback;
import io.mantisrx.shaded.org.apache.curator.framework.api.CuratorEvent;
import io.mantisrx.shaded.org.apache.curator.framework.imps.CuratorFrameworkState;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.cache.NodeCache;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.cache.NodeCacheListener;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;


/**
 * A monitor that monitors the status of Mantis masters.
 */
public class ZookeeperMasterMonitor extends BaseService implements MasterMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMasterMonitor.class);

    private final CuratorService curator;
    private final String masterPath;
    private final BehaviorSubject<MasterDescription> masterSubject;
    private final AtomicReference<MasterDescription> latestMaster = new AtomicReference<>();
    private final NodeCache nodeMonitor;

    public ZookeeperMasterMonitor(CuratorService curator, String masterPath) {
        this.curator = curator;
        this.masterPath = masterPath;
        this.masterSubject = BehaviorSubject.create();
        this.nodeMonitor = new NodeCache(curator.getCurator(), masterPath);
    }

    @Override
    public void start()  {
        try {
            if(curator.getCurator().getState() != CuratorFrameworkState.STARTED) {
                curator.start();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        nodeMonitor.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                retrieveMaster();
            }
        });

        try {
            nodeMonitor.start(true);
            onMasterNodeUpdated(nodeMonitor.getCurrentData() == null ? null : nodeMonitor.getCurrentData().getData());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        logger.info("The ZK master monitor has started");
    }

    private void onMasterNodeUpdated(@Nullable byte[] data) throws Exception {
        if (data != null) {
            logger.info("value was {}", new String(data));
            MasterDescription description = DefaultObjectMapper.getInstance().readValue(data, MasterDescription.class);
            logger.info("new master description = {}", description);
            latestMaster.set(description);
            masterSubject.onNext(description);
        } else {
            logger.info("looks like there's no master at the moment");
        }
    }

    private void retrieveMaster() {
        try {
            curator.getCurator()
                .sync()  // sync with ZK before reading
                .inBackground(
                    curator.getCurator()
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

    /**
     * Retrieves the currently cached latest master description.
     *
     * @return the latest MasterDescription or null
     */
    @Override
    @Nullable
    public MasterDescription getLatestMaster() {
        Preconditions.checkState(curator.getCurator().getState() == CuratorFrameworkState.STARTED, "ZookeeperMasterMonitor is currently not running but instead is at state %s", curator.getCurator().getState());
        return latestMaster.get();
    }

    @Override
    public void shutdown() {
        try {
            nodeMonitor.close();
            logger.info("ZK master monitor is shut down");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void enterActiveMode() {};
}
