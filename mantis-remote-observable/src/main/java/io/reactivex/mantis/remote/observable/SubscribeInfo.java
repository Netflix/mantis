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

package io.reactivex.mantis.remote.observable;

import java.util.Map;


public class SubscribeInfo {

    private String host;
    private int port;
    private String name;
    private Map<String, String> subscribeParameters;
    private int subscribeRetryAttempts;

    public SubscribeInfo(String host, int port, String name,
                         Map<String, String> subscribeParameters,
                         int subscribeRetryAttempts) {
        this.host = host;
        this.port = port;
        this.name = name;
        this.subscribeParameters = subscribeParameters;
        this.subscribeRetryAttempts = subscribeRetryAttempts;
    }

    public int getSubscribeRetryAttempts() {
        return subscribeRetryAttempts;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getSubscribeParameters() {
        return subscribeParameters;
    }
}
