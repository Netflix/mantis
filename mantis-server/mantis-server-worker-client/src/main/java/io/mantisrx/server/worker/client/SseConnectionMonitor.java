/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.worker.client;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;

public class SseConnectionMonitor {
    private List<Channel> sseConnections;

    public SseConnectionMonitor() {
        sseConnections = new ArrayList<>();
    }

    public void addConnection(Channel channel) {
        sseConnections.add(channel);
    }

    public void closeConnection() {
        Channel channel;
        for (int i=0;i<sseConnections.size();i++) {
            channel = sseConnections.get(i);
            channel.close();
        }
        sseConnections.clear();
    }
}
