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

import io.mantisrx.common.codec.Decoder;
import java.util.HashMap;
import java.util.Map;
import rx.functions.Action0;
import rx.functions.Action2;
import rx.subjects.PublishSubject;


public class ConnectToObservable<T> extends ConnectToConfig {

    private Decoder<T> decoder;
    private Action2<T, Throwable> deocdingErrorHandler;

    ConnectToObservable(Builder<T> builder) {
        super(builder.host, builder.port,
                builder.name,
                builder.subscribeParameters,
                builder.subscribeRetryAttempts,
                builder.suppressDecodingErrors,
                builder.connectionDisconnectCallback,
                builder.closeTrigger);
        this.decoder = builder.decoder;
        this.deocdingErrorHandler = builder.deocdingErrorHandler;
    }

    public Decoder<T> getDecoder() {
        return decoder;
    }

    public Action2<T, Throwable> getDeocdingErrorHandler() {
        return deocdingErrorHandler;
    }

    public static class Builder<T> {

        private String host;
        private int port;
        private String name;
        private Decoder<T> decoder;
        private Map<String, String> subscribeParameters = new HashMap<>();
        private int subscribeRetryAttempts = 3;
        private Action2<T, Throwable> deocdingErrorHandler = new Action2<T, Throwable>() {
            @Override
            public void call(T t1, Throwable t2) {
                t2.printStackTrace();
            }
        };
        private boolean suppressDecodingErrors = false;
        private Action0 connectionDisconnectCallback = new Action0() {
            @Override
            public void call() {}
        };
        private PublishSubject<Integer> closeTrigger = PublishSubject.create();

        public Builder() {}

        public Builder(Builder<T> config) {
            this.host = config.host;
            this.port = config.port;
            this.name = config.name;
            this.decoder = config.decoder;
            this.subscribeParameters.putAll(config.subscribeParameters);
            this.subscribeRetryAttempts = config.subscribeRetryAttempts;
            this.deocdingErrorHandler = config.deocdingErrorHandler;
            this.suppressDecodingErrors = config.suppressDecodingErrors;
        }

        public Builder<T> host(String host) {
            this.host = host;
            return this;
        }

        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> closeTrigger(PublishSubject<Integer> closeTrigger) {
            this.closeTrigger = closeTrigger;
            return this;
        }

        public Builder<T> connectionDisconnectCallback(Action0 connectionDisconnectCallback) {
            this.connectionDisconnectCallback = connectionDisconnectCallback;
            return this;
        }

        public Builder<T> deocdingErrorHandler(Action2<T, Throwable> handler, boolean suppressDecodingErrors) {
            this.deocdingErrorHandler = handler;
            this.suppressDecodingErrors = suppressDecodingErrors;
            return this;
        }

        public Builder<T> name(String name) {
            this.name = name;
            this.subscribeParameters.put("groupId", name); //used with modern server for routing
            return this;
        }

        public Builder<T> slotId(String slotId) {
            this.subscribeParameters.put("slotId", slotId);
            return this;
        }

        public Builder<T> availabilityZone(String availabilityZone) {
            this.subscribeParameters.put("availabilityZone", availabilityZone);
            return this;
        }

        public Builder<T> decoder(Decoder<T> decoder) {
            this.decoder = decoder;
            return this;
        }

        public Builder<T> subscribeParameters(Map<String, String> subscribeParameters) {
            this.subscribeParameters.putAll(subscribeParameters);
            return this;
        }

        public Builder<T> subscribeAttempts(int subscribeAttempts) {
            this.subscribeRetryAttempts = subscribeAttempts;
            return this;
        }

        public ConnectToObservable<T> build() {
            return new ConnectToObservable<T>(this);
        }
    }

}
