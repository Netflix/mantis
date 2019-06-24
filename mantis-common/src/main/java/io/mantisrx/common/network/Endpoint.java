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

package io.mantisrx.common.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action0;
import rx.functions.Action1;


public class Endpoint {

    private static final Logger logger = LoggerFactory.getLogger(Endpoint.class);
    final String slotId;
    private final String host;
    private final int port;
    private final Action0 completedCallback;

    private final Action1<Throwable> errorCallback;

    public Endpoint(final String host, final int port) {
        this(host, port, uniqueHost(host, port, null));
    }

    public Endpoint(final String host, final int port, final String slotId) {
        this(host,
                port,
                slotId,
                new Action0() {
                    @Override
                    public void call() {
                        logger.info("onComplete received for {}:{} slotId {}", host, port, slotId);
                    }
                },
                new Action1<Throwable>() {
                    @Override
                    public void call(Throwable t1) {
                        logger.warn("onError for {}:{} slotId {} err {}", host, port, slotId, t1.getMessage(), t1);
                    }
                });
    }

    public Endpoint(final String host, final int port, final Action0 completedCallback,
                    final Action1<Throwable> errorCallback) {
        this(host, port, uniqueHost(host, port, null), completedCallback, errorCallback);
    }

    public Endpoint(final String host, final int port, final String slotId, final Action0 completedCallback,
                    final Action1<Throwable> errorCallback) {
        this.host = host;
        this.port = port;
        this.slotId = slotId;
        this.completedCallback = completedCallback;
        this.errorCallback = errorCallback;
    }

    public static String uniqueHost(final String host, final int port, final String slotId) {
        if (slotId == null) {
            return host + ":" + port;
        }
        return host + ":" + port + ":" + slotId;
    }

    public String getSlotId() {
        return slotId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Action0 getCompletedCallback() {
        return completedCallback;
    }

    public Action1<Throwable> getErrorCallback() {
        return errorCallback;
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", slotId='" + slotId + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        result = prime * result + ((slotId == null) ? 0 : slotId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Endpoint other = (Endpoint) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (port != other.port)
            return false;
        if (slotId == null) {
            if (other.slotId != null)
                return false;
        } else if (!slotId.equals(other.slotId))
            return false;
        return true;
    }
}
