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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;

public class SseConnectionMonitor<I, O> {
    private static SseConnectionMonitor INSTANCE;
    private Map<Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>>, Channel> sseConnections;


    private SseConnectionMonitor() {
        sseConnections = new ConcurrentHashMap<>();
    }

    public static SseConnectionMonitor getInstance() {
        if(INSTANCE == null) {
            INSTANCE = new SseConnectionMonitor();
        }

        return INSTANCE;
    }

    public void addConnection(Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> observableConnection, Channel channel) {
        sseConnections.put(observableConnection, channel);
    }

    public void closeConnection(Observable<ObservableConnection<HttpClientResponse<O>, HttpClientRequest<I>>> observableConnection) {
        Channel channel = sseConnections.get(observableConnection);
        channel.close();
    }
}
