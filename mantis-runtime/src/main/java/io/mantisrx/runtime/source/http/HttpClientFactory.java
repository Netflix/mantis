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

package io.mantisrx.runtime.source.http;

import mantis.io.reactivex.netty.client.RxClient.ServerInfo;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;


/**
 * A factory that creates new {@link mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest} for a given server, which is uniquely identified
 * by its host name and a given port.
 *
 * @param <R> The request entity's type
 * @param <E> The response entity's type
 */
public interface HttpClientFactory<R, E> {

    HttpClient<R, E> createClient(ServerInfo server);
}
