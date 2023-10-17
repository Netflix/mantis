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
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import io.mantisrx.shaded.org.apache.curator.framework.CuratorFramework;
import io.mantisrx.shaded.org.apache.curator.framework.recipes.cache.CuratorCache;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;


/**
 * A monitor that monitors the status of Mantis masters.
 */
public class ZookeeperMasterMonitor extends AbstractIdleService implements MasterMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMasterMonitor.class);

    private final CuratorFramework curator;
    private final String masterPath;
    private final BehaviorSubject<MasterDescription> masterSubject;
    private final AtomicReference<MasterDescription> latestMaster = new AtomicReference<>();
    private final CuratorCache nodeMonitor;
    private final JsonSerializer jsonSerializer;

    public ZookeeperMasterMonitor(CuratorFramework curator, String masterPath) {
        this.curator = curator;
        this.masterPath = masterPath;
        this.masterSubject = BehaviorSubject.create();
        this.nodeMonitor = CuratorCache.build(curator, masterPath);
        this.jsonSerializer = new JsonSerializer();
    }

    @Override
    public void startUp() throws Exception {
        // set the initial master
        nodeMonitor.listenable().addListener((type, oldData, newData) -> {
            try {
                onMasterNodeUpdated(newData.getData());
            } catch (Exception e) {
                logger.error("Failed to retrieve updated master information: " + e.getMessage(), e);
            }
        });

        nodeMonitor.start();
        logger.info("The ZK master monitor has started");
    }

    private void onMasterNodeUpdated(@Nullable byte[] data) throws Exception {
        if (data != null) {
            logger.info("value was {}", new String(data));
            MasterDescription description = DefaultObjectMapper.getInstance()
                .readValue(data, MasterDescription.class);
            logger.info("new master description = {}", description);
            latestMaster.set(description);
            masterSubject.onNext(description);
        } else {
            logger.info("looks like there's no master at the moment");
        }
    }

    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return masterSubject;
    }

    /**
     * @return
     */
    @Override
    @Nullable
    public MasterDescription getLatestMaster() {
        Preconditions.checkState(isRunning(),
            "ZookeeperMasterMonitor is currently not running but instead is at state %s", state());
        return latestMaster.get();
    }

    @Override
    public void shutDown() throws Exception {
        nodeMonitor.close();
        logger.info("ZK master monitor is shut down");
    }
}
