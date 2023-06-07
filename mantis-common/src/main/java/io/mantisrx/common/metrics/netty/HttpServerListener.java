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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.metrics.HttpServerMetricEventsListener;
import mantis.io.reactivex.netty.server.ServerMetricsEvent;


/**
 * @author Neeraj Joshi
 */
public class HttpServerListener extends TcpServerListener<ServerMetricsEvent<?>> {


    private final Gauge requestBacklog;
    private final Gauge inflightRequests;
    private final Counter processedRequests;
    private final Counter failedRequests;
    private final Counter responseWriteFailed;
    private AtomicLong inflightRequestsValue = new AtomicLong(0);
    private AtomicLong requestBacklogValue = new AtomicLong(0);


    private final HttpServerMetricEventsListenerImpl delegate;

    protected HttpServerListener(String monitorId) {
        super(monitorId);

        String groupName = "httpServer_" + monitorId;

        requestBacklog = Gauge.builder(groupName + "_requestBacklog", this::getRequestBacklog)
            .register(Metrics.globalRegistry);
        inflightRequests = Gauge.builder(groupName +"_inflightRequests", this::getInflightRequests)
            .register(Metrics.globalRegistry);
        failedRequests = Counter.builder(groupName + "_failedRequests")
            .register(Metrics.globalRegistry);
        processedRequests = Counter.builder(groupName + "_processedRequests")
            .register(Metrics.globalRegistry);
        responseWriteFailed = Counter.builder(groupName + "_responseWriteFailed")
            .register(Metrics.globalRegistry);

        delegate = new HttpServerMetricEventsListenerImpl();
    }

    public static HttpServerListener newHttpListener(String monitorId) {
        return new HttpServerListener(monitorId);
    }

    @Override
    public void onEvent(ServerMetricsEvent<?> event, long duration, TimeUnit timeUnit, Throwable throwable,
                        Object value) {
        delegate.onEvent(event, duration, timeUnit, throwable, value);
    }

    public long getRequestBacklog() {
        return requestBacklogValue.get();
    }

    public long getInflightRequests() {
        return inflightRequestsValue.get();
    }

    public double getProcessedRequests() {
        return processedRequests.count();
    }


    public double getFailedRequests() {
        return failedRequests.count();
    }

    //    public Timer getResponseWriteTimes() {
    //        return responseWriteTimes;
    //    }
    //
    //    public Timer getRequestReadTimes() {
    //        return requestReadTimes;
    //    }


    public double getResponseWriteFailed() {
        return responseWriteFailed.count();
    }

    private class HttpServerMetricEventsListenerImpl extends HttpServerMetricEventsListener {

        @Override
        protected void onRequestHandlingFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            processedRequests.increment();
            inflightRequestsValue = new AtomicLong(inflightRequestsValue.get() - 1);
            failedRequests.increment();
        }

        @Override
        protected void onRequestHandlingSuccess(long duration, TimeUnit timeUnit) {
            inflightRequestsValue = new AtomicLong(inflightRequestsValue.get() - 1);
            processedRequests.increment();
        }

        @Override
        protected void onResponseContentWriteSuccess(long duration, TimeUnit timeUnit) {
            // responseWriteTimes.record(duration, timeUnit);
        }

        @Override
        protected void onResponseHeadersWriteSuccess(long duration, TimeUnit timeUnit) {
            // responseWriteTimes.record(duration, timeUnit);
        }

        @Override
        protected void onResponseContentWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            responseWriteFailed.increment();
        }

        @Override
        protected void onResponseHeadersWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            responseWriteFailed.increment();
        }

        @Override
        protected void onRequestReceiveComplete(long duration, TimeUnit timeUnit) {
            //  requestReadTimes.record(duration, timeUnit);
        }

        @Override
        protected void onRequestHandlingStart(long duration, TimeUnit timeUnit) {
            requestBacklogValue = new AtomicLong(requestBacklogValue.get() - 1);
        }

        @Override
        protected void onNewRequestReceived() {
            requestBacklogValue = new AtomicLong(requestBacklogValue.get() + 1);
            inflightRequestsValue = new AtomicLong(inflightRequestsValue.get() + 1);
        }

        @Override
        protected void onConnectionHandlingFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpServerListener.this.onConnectionHandlingFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onConnectionHandlingSuccess(long duration, TimeUnit timeUnit) {
            HttpServerListener.this.onConnectionHandlingSuccess(duration, timeUnit);
        }

        @Override
        protected void onConnectionHandlingStart(long duration, TimeUnit timeUnit) {
            HttpServerListener.this.onConnectionHandlingStart(duration, timeUnit);
        }

        @Override
        protected void onNewClientConnected() {
            HttpServerListener.this.onNewClientConnected();
        }

        @Override
        protected void onByteRead(long bytesRead) {
            HttpServerListener.this.onByteRead(bytesRead);
        }

        @Override
        protected void onFlushFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpServerListener.this.onFlushFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onFlushSuccess(long duration, TimeUnit timeUnit) {
            HttpServerListener.this.onFlushSuccess(duration, timeUnit);
        }

        @Override
        protected void onFlushStart() {
            HttpServerListener.this.onFlushStart();
        }

        @Override
        protected void onWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpServerListener.this.onWriteFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onWriteSuccess(long duration, TimeUnit timeUnit, long bytesWritten) {
            HttpServerListener.this.onWriteSuccess(duration, timeUnit, bytesWritten);
        }

        @Override
        protected void onWriteStart() {
            HttpServerListener.this.onWriteStart();
        }
    }
}
