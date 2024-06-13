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

package io.mantisrx.server.master;

import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.master.MasterDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LeadershipManagerLocalImpl implements ILeadershipManager {

    private static final Logger logger = LoggerFactory.getLogger(LeadershipManagerLocalImpl.class);
    private final MasterDescription masterDescription;
    private volatile boolean isLeader = true;
    private volatile boolean isReady = false;

    public LeadershipManagerLocalImpl(MasterDescription masterDescription) {
        this.masterDescription = masterDescription;
    }

    @Override
    public void becomeLeader() {
        logger.info("Becoming leader now");
        isLeader = true;
    }

    @Override
    public boolean isLeader() {
        logger.debug("is leader? {}", isLeader);
        return isLeader;
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @Override
    public void setLeaderReady() {
        logger.info("marking leader READY");
        isReady = true;
    }

    @Override
    public void stopBeingLeader() {
        logger.info("Asked to stop being leader now");
        isReady = false;
        isLeader = false;
    }

    @Override
    public MasterDescription getDescription() {
        return masterDescription;
    }
}
