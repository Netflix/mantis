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

import io.netty.bootstrap.Bootstrap;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientBuilder;

public class MantisHttpClientBuilder<I, O> extends HttpClientBuilder<I, O> {
    public MantisHttpClientBuilder(String host, int port) {
        super(host, port, new Bootstrap());
    }

    @Override
    protected HttpClient<I, O> createClient() {
        if (null == super.poolBuilder) {
            return new MantisHttpClientImpl<I, O>(this.getOrCreateName(), this.serverInfo, this.bootstrap, this.pipelineConfigurator, this.clientConfig, this.channelFactory, this.connectionFactory, this.eventsSubject);
        } else {
            return new MantisHttpClientImpl<I, O>(this.getOrCreateName(), this.serverInfo, this.bootstrap, this.pipelineConfigurator, this.clientConfig, this.poolBuilder, this.eventsSubject);
        }
    }
}
