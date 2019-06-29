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

import java.util.HashMap;
import java.util.Map;

import io.mantisrx.common.network.Endpoint;
import rx.functions.Action0;
import rx.subjects.PublishSubject;


public abstract class ConnectToConfig {

    private Endpoint endpoint;
    private String name;
    private Map<String, String> subscribeParameters = new HashMap<String, String>();
    private int subscribeAttempts;
    private boolean suppressDecodingErrors = false;
    private Action0 connectionDisconnectCallback;
    private PublishSubject<Integer> closeTrigger;

    public ConnectToConfig(String host, int port, String name,
                           Map<String, String> subscribeParameters,
                           int subscribeAttempts,
                           boolean suppressDecodingErrors,
                           Action0 connectionDisconnectCallback,
                           PublishSubject<Integer> closeTrigger) {
        endpoint = new Endpoint(host, port);
        this.name = name;
        this.subscribeParameters.putAll(subscribeParameters);
        this.subscribeAttempts = subscribeAttempts;
        this.suppressDecodingErrors = suppressDecodingErrors;
        this.connectionDisconnectCallback = connectionDisconnectCallback;
        this.closeTrigger = closeTrigger;
    }

    public Action0 getConnectionDisconnectCallback() {
        return connectionDisconnectCallback;
    }

    public PublishSubject<Integer> getCloseTrigger() {
        return closeTrigger;
    }

    public String getHost() {
        return endpoint.getHost();
    }

    public int getPort() {
        return endpoint.getPort();
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getSubscribeParameters() {
        return subscribeParameters;
    }

    public int getSubscribeAttempts() {
        return subscribeAttempts;
    }

    public boolean isSuppressDecodingErrors() {
        return suppressDecodingErrors;
    }
}
