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

import static com.mantisrx.common.utils.MantisMetricStringConstants.GROUP_ID_TAG;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.metrics.HttpClientMetricEventsListener;


/**
 * @author Neeraj Joshi
 */
public class HttpClientListener extends TcpClientListener<ClientMetricsEvent<?>> {

    private final Gauge requestBacklog;
    private final Gauge inflightRequests;
    private final Counter processedRequests;
    private final Counter requestWriteFailed;
    private final Counter failedResponses;
    //    private final Timer requestWriteTimes;
    //    private final Timer responseReadTimes;
    //    private final Timer requestProcessingTimes;

    private final HttpClientMetricEventsListenerImpl delegate = new HttpClientMetricEventsListenerImpl();

    protected HttpClientListener(String monitorId) {
        super(monitorId);

        final String metricsGroup = "httpClient";
        final String idValue = Optional.ofNullable(monitorId).orElse("none");
        final BasicTag idTag = new BasicTag(GROUP_ID_TAG, idValue);
        Metrics m = new Metrics.Builder()
                .id(metricsGroup, idTag)
                .addGauge("requestBacklog")
                .addGauge("inflightRequests")
                .addCounter("processedRequests")
                .addCounter("requestWriteFailed")
                .addCounter("failedResponses")

                .build();

        requestBacklog = m.getGauge("requestBacklog");
        inflightRequests = m.getGauge("inflightRequests");
        //        requestWriteTimes = newTimer("requestWriteTimes");
        //        responseReadTimes = newTimer("responseReadTimes");
        processedRequests = m.getCounter("processedRequests");
        requestWriteFailed = m.getCounter("requestWriteFailed");
        failedResponses = m.getCounter("failedResponses");
        //  requestProcessingTimes = newTimer("requestProcessingTimes");
    }

    public static HttpClientListener newHttpListener(String monitorId) {
        return new HttpClientListener(monitorId);
    }

    @Override
    public void onEvent(ClientMetricsEvent<?> event, long duration, TimeUnit timeUnit, Throwable throwable,
                        Object value) {
        delegate.onEvent(event, duration, timeUnit, throwable, value);
    }

    public long getRequestBacklog() {
        return (long) requestBacklog.doubleValue();
    }

    public long getInflightRequests() {
        return (long) inflightRequests.doubleValue();
    }

    public long getProcessedRequests() {
        return processedRequests.value();
    }

    public long getRequestWriteFailed() {
        return requestWriteFailed.value();
    }

    public long getFailedResponses() {
        return failedResponses.value();
    }

    //    public Timer getRequestWriteTimes() {
    //        return requestWriteTimes;
    //    }
    //
    //    public Timer getResponseReadTimes() {
    //        return responseReadTimes;
    //    }

    private class HttpClientMetricEventsListenerImpl extends HttpClientMetricEventsListener {

        @Override
        protected void onRequestProcessingComplete(long duration, TimeUnit timeUnit) {
            // requestProcessingTimes.record(duration, timeUnit);
        }

        @Override
        protected void onResponseReceiveComplete(long duration, TimeUnit timeUnit) {
            inflightRequests.decrement();
            processedRequests.increment();
            //  responseReadTimes.record(duration, timeUnit);
        }

        @Override
        protected void onResponseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            inflightRequests.decrement();
            processedRequests.increment();
            failedResponses.increment();
        }

        @Override
        protected void onRequestWriteComplete(long duration, TimeUnit timeUnit) {
            //requestWriteTimes.record(duration, timeUnit);
        }

        @Override
        protected void onRequestContentWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            requestWriteFailed.increment();
        }

        @Override
        protected void onRequestHeadersWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            requestWriteFailed.increment();
        }

        @Override
        protected void onRequestHeadersWriteStart() {
            requestBacklog.decrement();
        }

        @Override
        protected void onRequestSubmitted() {
            requestBacklog.increment();
            inflightRequests.increment();
        }

        @Override
        protected void onByteRead(long bytesRead) {
            HttpClientListener.this.onByteRead(bytesRead);
        }

        @Override
        protected void onFlushFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpClientListener.this.onFlushFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onFlushSuccess(long duration, TimeUnit timeUnit) {
            HttpClientListener.this.onFlushSuccess(duration, timeUnit);
        }

        @Override
        protected void onFlushStart() {
            HttpClientListener.this.onFlushStart();
        }

        @Override
        protected void onWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpClientListener.this.onWriteFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onWriteSuccess(long duration, TimeUnit timeUnit, long bytesWritten) {
            HttpClientListener.this.onWriteSuccess(duration, timeUnit, bytesWritten);
        }

        @Override
        protected void onWriteStart() {
            HttpClientListener.this.onWriteStart();
        }

        @Override
        protected void onPoolReleaseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpClientListener.this.onPoolReleaseFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onPoolReleaseSuccess(long duration, TimeUnit timeUnit) {
            HttpClientListener.this.onPoolReleaseSuccess(duration, timeUnit);
        }

        @Override
        protected void onPoolReleaseStart() {
            HttpClientListener.this.onPoolReleaseStart();
        }

        @Override
        protected void onPooledConnectionEviction() {
            HttpClientListener.this.onPooledConnectionEviction();
        }

        @Override
        protected void onPooledConnectionReuse(long duration, TimeUnit timeUnit) {
            HttpClientListener.this.onPooledConnectionReuse(duration, timeUnit);
        }

        @Override
        protected void onPoolAcquireFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpClientListener.this.onPoolAcquireFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onPoolAcquireSuccess(long duration, TimeUnit timeUnit) {
            HttpClientListener.this.onPoolAcquireSuccess(duration, timeUnit);
        }

        @Override
        protected void onPoolAcquireStart() {
            HttpClientListener.this.onPoolAcquireStart();
        }

        @Override
        protected void onConnectionCloseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpClientListener.this.onConnectionCloseFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onConnectionCloseSuccess(long duration, TimeUnit timeUnit) {
            HttpClientListener.this.onConnectionCloseSuccess(duration, timeUnit);
        }

        @Override
        protected void onConnectionCloseStart() {
            HttpClientListener.this.onConnectionCloseStart();
        }

        @Override
        protected void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
            HttpClientListener.this.onConnectFailed(duration, timeUnit, throwable);
        }

        @Override
        protected void onConnectSuccess(long duration, TimeUnit timeUnit) {
            HttpClientListener.this.onConnectSuccess(duration, timeUnit);
        }

        @Override
        protected void onConnectStart() {
            HttpClientListener.this.onConnectStart();
        }
    }
}
