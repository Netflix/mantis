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

package io.mantisrx.server.core.highavailability;

import com.typesafe.config.Config;
import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Value
public class NodeSettings {
    String host;
    String ip;

    int consolePort;
    int apiPort;
    int scheduleInfoPort;
    int apiPortV2;
    String apiStatusURI;

    public static NodeSettings fromConfig(Config config) {
        try {
            return new NodeSettings(
                config.hasPath("host") ? config.getString("host") : InetAddress.getLocalHost().getHostName(),
                config.hasPath("ip") ? config.getString("host") : InetAddress.getLocalHost().getHostAddress(),

                config.getInt("consoleport"),
                config.getInt("apiport"),
                config.getInt("schedInfoPort"),
                config.getInt("apiportv2"),
                config.getString("apiStatusURI"));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
