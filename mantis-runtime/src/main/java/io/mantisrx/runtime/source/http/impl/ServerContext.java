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

package io.mantisrx.runtime.source.http.impl;

import mantis.io.reactivex.netty.client.RxClient.ServerInfo;


/**
 * The context that provides contextual information for a value. In particular, the information
 * about a server
 *
 * @param <T> The type of the value associated with the context
 */
public class ServerContext<T> {

    private final ServerInfo server;
    private final T value;


    public ServerContext(ServerInfo server, T value) {
        this.server = server;
        this.value = value;
    }

    public ServerInfo getServer() {
        return server;
    }

    public T getValue() {
        return value;
    }
}
