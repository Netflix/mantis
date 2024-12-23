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
package io.mantisrx.extensions.dynamodb;

import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


@Slf4j
public class DynamoDBMasterMonitor extends BaseService implements MasterMonitor {
    private final DynamoDBMasterMonitorSingleton singleton;

    /**
     * Creates a MasterMonitor backed by DynamoDB. This should be used if you are using a {@link DynamoDBLeaderElector}
     */
    public DynamoDBMasterMonitor() {
        this.singleton = DynamoDBMasterMonitorSingleton.getInstance();
    }

    @Override
    public void start() {}

    @Override
    public void shutdown() {}

    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return singleton.getMasterSubject();
    }

    /**
     * Returns the latest master if there's one. If there has been no master in recent history, then
     * this return null.
     *
     * @return Latest description of the master
     */
    @Override
    public MasterDescription getLatestMaster() {
        return singleton.getMasterSubject().getValue();
    }
}
