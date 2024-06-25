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
package io.mantisrx.server.core;

import io.mantisrx.server.core.master.MasterDescription;

public interface ILeadershipManager {

    // execute all actions when becoming a leader
    void becomeLeader();

    // actions to execute when losing leadership
    void stopBeingLeader();

    // am I the current leader?
    boolean isLeader();

    // check if leader is bootstrapped and ready after becoming leader
    boolean isReady();

    // set Leader is bootstrapped and ready
    void setLeaderReady();

    // return MasterDescription of node executing this function
    MasterDescription getDescription();
}
