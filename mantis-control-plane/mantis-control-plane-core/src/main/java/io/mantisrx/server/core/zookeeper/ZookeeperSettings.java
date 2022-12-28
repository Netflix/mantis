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

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Value;

@Value
public class ZookeeperSettings {
    Config config;
    Duration connectionTimeout;
    Duration connectionRetrySleepTime;
    int connectionRetryCount;
    String connectString;
    String rootPath;
    String leaderAnnouncementPath;

    public ZookeeperSettings(Config config) {
        this.config = config;
        this.connectionTimeout = config.getDuration("connectionTimeout");
        this.connectionRetrySleepTime = config.getDuration("connectionRetrySleepTime");
        this.connectionRetryCount = config.getInt("connectionRetryCount");
        this.connectString = config.getString("connectString");
        this.rootPath = config.getString("rootPath");
        this.leaderAnnouncementPath = config.getString("leaderAnnouncementPath");
    }
}
