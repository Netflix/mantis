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

package io.mantisrx.common.metrics.netty;

import java.util.concurrent.TimeUnit;

import mantis.io.reactivex.netty.client.ClientMetricsEvent;


/**
 * @author Neeraj Joshi
 */
public class UdpClientListener extends TcpClientListener<ClientMetricsEvent<?>> {

    protected UdpClientListener(String monitorId) {
        super(monitorId);
    }

    public static UdpClientListener newUdpListener(String monitorId) {
        return new UdpClientListener(monitorId);
    }

    @Override
    public void onEvent(ClientMetricsEvent<?> event, long duration, TimeUnit timeUnit, Throwable throwable,
                        Object value) {
        if (event.getType() instanceof ClientMetricsEvent.EventType) {
            super.onEvent(event, duration, timeUnit, throwable, value);
        }
    }
}
