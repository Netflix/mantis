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
import rx.functions.Action3;
import rx.subjects.PublishSubject;


public class ConnectToGroupedObservable<K, V> extends ConnectToConfig {

    private Decoder<K> keyDecoder;
    private Decoder<V> valueDecoder;
    private Action3<K, V, Throwable> deocdingErrorHandler;

    ConnectToGroupedObservable(Builder<K, V> builder) {
        super(builder.host, builder.port,
                builder.name,
                builder.subscribeParameters,
                builder.subscribeAttempts,
                builder.suppressDecodingErrors,
                builder.connectionDisconnectCallback,
                builder.closeTrigger);
        this.keyDecoder = builder.keyDecoder;
        this.valueDecoder = builder.valueDecoder;
        this.deocdingErrorHandler = builder.deocdingErrorHandler;
    }

    public Decoder<K> getKeyDecoder() {
        return keyDecoder;
    }

    public Decoder<V> getValueDecoder() {
        return valueDecoder;
    }

    public Action3<K, V, Throwable> getDeocdingErrorHandler() {
        return deocdingErrorHandler;
    }

    public static class Builder<K, V> {

        private String host;
        private int port;
        private String name;
        private Decoder<K> keyDecoder;
        private Decoder<V> valueDecoder;
        private Map<String, String> subscribeParameters = new HashMap<>();
        private int subscribeAttempts = 3;
        private Action3<K, V, Throwable> deocdingErrorHandler = new Action3<K, V, Throwable>() {
            @Override
            public void call(K key, V value, Throwable t2) {
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

        public Builder(Builder<K, V> config) {
            this.host = config.host;
            this.port = config.port;
            this.name = config.name;
            this.keyDecoder = config.keyDecoder;
            this.valueDecoder = config.valueDecoder;
            this.subscribeParameters.putAll(config.subscribeParameters);
            this.subscribeAttempts = config.subscribeAttempts;
            this.deocdingErrorHandler = config.deocdingErrorHandler;
            this.suppressDecodingErrors = config.suppressDecodingErrors;
        }

        public Builder<K, V> host(String host) {
            this.host = host;
            return this;
        }

        public Builder<K, V> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<K, V> closeTrigger(PublishSubject<Integer> closeTrigger) {
            this.closeTrigger = closeTrigger;
            return this;
        }

        public Builder<K, V> connectionDisconnectCallback(Action0 connectionDisconnectCallback) {
            this.connectionDisconnectCallback = connectionDisconnectCallback;
            return this;
        }

        public Builder<K, V> deocdingErrorHandler(Action3<K, V, Throwable> handler, boolean suppressDecodingErrors) {
            this.deocdingErrorHandler = handler;
            this.suppressDecodingErrors = suppressDecodingErrors;
            return this;
        }

        public Builder<K, V> name(String name) {
            this.name = name;
            this.subscribeParameters.put("groupId", name);//used with modern server for routing
            return this;
        }

        public Builder<K, V> slotId(String slotId) {
            this.subscribeParameters.put("slotId", slotId);
            return this;
        }

        public Builder<K, V> keyDecoder(Decoder<K> keyDecoder) {
            this.keyDecoder = keyDecoder;
            return this;
        }

        public Builder<K, V> valueDecoder(Decoder<V> valueDecoder) {
            this.valueDecoder = valueDecoder;
            return this;
        }

        public Builder<K, V> subscribeParameters(Map<String, String> subscribeParameters) {
            this.subscribeParameters.putAll(subscribeParameters);
            return this;
        }

        public Builder<K, V> subscribeAttempts(int subscribeAttempts) {
            this.subscribeAttempts = subscribeAttempts;
            return this;
        }

        public ConnectToGroupedObservable<K, V> build() {
            return new ConnectToGroupedObservable<K, V>(this);
        }
    }
}
