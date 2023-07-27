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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
//import io.mantisrx.common.metrics.Counter;
//import io.mantisrx.common.metrics.Gauge;
//import io.mantisrx.common.metrics.Metrics;
//import io.mantisrx.common.metrics.MetricsRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import mantis.io.reactivex.netty.metrics.ServerMetricEventsListener;
import mantis.io.reactivex.netty.server.ServerMetricsEvent;


/**
 * @author Neeraj Joshi
 */
public class TcpServerListener<T extends ServerMetricsEvent<?>> extends ServerMetricEventsListener<T> {

    private final Gauge liveConnections;
    private final AtomicLong inflightConnectionsValue = new AtomicLong(0);
    private final Gauge inflightConnections;
    private final AtomicLong liveConnectionsValue = new AtomicLong(0);
    private final Counter failedConnections;
    //private final Timer connectionProcessingTimes;
    private final Gauge pendingConnectionClose;
    private final AtomicLong pendingConnectionCloseValue = new AtomicLong(0);
    private final Counter failedConnectionClose;
    //private final Timer connectionCloseTimes;

    private final Gauge pendingWrites;
    private final AtomicLong pendingWritesValue = new AtomicLong(0);
    private final Gauge pendingFlushes;
    private final AtomicLong pendingFlushesValue = new AtomicLong(0);

    private final Counter bytesWritten;
    //private final Timer writeTimes;
    private final Counter bytesRead;
    private final Counter failedWrites;
    private final Counter failedFlushes;
    //private final Timer flushTimes;


    protected TcpServerListener(String monitorId, MeterRegistry micrometerRegistry) {
        super();

        String groupName = "tcpServer" + "-" + monitorId;

        liveConnections = Gauge.builder(groupName + "_liveConnections", liveConnectionsValue::get)
                .register(micrometerRegistry);

        inflightConnections = Gauge.builder(groupName + "_inflightConnections", inflightConnectionsValue::get)
                .register(micrometerRegistry);

        failedConnections = Counter.builder(groupName + "_failedConnections")
                .register(micrometerRegistry);

        pendingConnectionClose = Gauge.builder(groupName + "_pendingConnectionClose", pendingConnectionCloseValue::get)
                .register(micrometerRegistry);

        failedConnectionClose = Counter.builder(groupName + "_failedConnectionClose")
                .register(micrometerRegistry);

        //        connectionProcessingTimes = newTimer("connectionProcessingTimes");
        //        connectionCloseTimes = newTimer("connectionCloseTimes");

        pendingWrites = Gauge.builder(groupName + "_pendingWrites", pendingWritesValue::get)
                .register(micrometerRegistry);

        pendingFlushes = Gauge.builder(groupName + "_pendingFlushes", pendingFlushesValue::get)
                .register(micrometerRegistry);

        bytesWritten = Counter.builder(groupName + "_bytesWritten")
                .register(micrometerRegistry);

        //   writeTimes = newTimer("writeTimes");
        bytesRead = Counter.builder(groupName + "_bytesRead")
                .register(micrometerRegistry);

        failedWrites = Counter.builder(groupName + "_failedWrites")
                .register(micrometerRegistry);

        failedFlushes = Counter.builder(groupName + "_failedFlushes")
                .register(micrometerRegistry);
        //    flushTimes = newTimer("flushTimes");
    }


    public static TcpServerListener<ServerMetricsEvent<ServerMetricsEvent.EventType>> newListener(String monitorId, MeterRegistry micrometerRegistry) {
        return new TcpServerListener<ServerMetricsEvent<ServerMetricsEvent.EventType>>(monitorId, micrometerRegistry);
    }

    @Override
    protected void onConnectionHandlingFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        inflightConnectionsValue.decrementAndGet();
        failedConnections.increment();
    }

    @Override
    protected void onConnectionHandlingSuccess(long duration, TimeUnit timeUnit) {
        inflightConnectionsValue.decrementAndGet();
        //connectionProcessingTimes.record(duration, timeUnit);
    }

    @Override
    protected void onConnectionHandlingStart(long duration, TimeUnit timeUnit) {
        inflightConnectionsValue.incrementAndGet();
    }

    @Override
    protected void onConnectionCloseStart() {
        pendingConnectionCloseValue.incrementAndGet();
    }

    @Override
    protected void onConnectionCloseSuccess(long duration, TimeUnit timeUnit) {
        liveConnectionsValue.incrementAndGet();
        pendingConnectionCloseValue.decrementAndGet();
        //connectionCloseTimes.record(duration, timeUnit);
    }

    @Override
    protected void onConnectionCloseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        liveConnectionsValue.decrementAndGet();
        pendingConnectionCloseValue.decrementAndGet();
        // connectionCloseTimes.record(duration, timeUnit);
        failedConnectionClose.increment();
    }

    @Override
    protected void onNewClientConnected() {
        liveConnectionsValue.incrementAndGet();
    }

    @Override
    protected void onByteRead(long bytesRead) {
        this.bytesRead.increment(bytesRead);
    }

    @Override
    protected void onFlushFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingFlushesValue.decrementAndGet();
        failedFlushes.increment();
    }

    @Override
    protected void onFlushSuccess(long duration, TimeUnit timeUnit) {
        pendingFlushesValue.decrementAndGet();
        //   flushTimes.record(duration, timeUnit);
    }

    @Override
    protected void onFlushStart() {
        pendingFlushesValue.incrementAndGet();
    }

    @Override
    protected void onWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingWritesValue.decrementAndGet();
        failedWrites.increment();
    }

    @Override
    protected void onWriteSuccess(long duration, TimeUnit timeUnit, long bytesWritten) {
        pendingWritesValue.decrementAndGet();
        this.bytesWritten.increment(bytesWritten);
        //writeTimes.record(duration, timeUnit);
    }

    @Override
    protected void onWriteStart() {
        pendingWritesValue.incrementAndGet();
    }

    @Override
    public void onCompleted() {

    }

    @Override
    public void onSubscribe() {

    }

    public long getLiveConnections() {
        return liveConnectionsValue.get();
    }

    public long getInflightConnections() {
        return inflightConnectionsValue.get();
    }

    public double getFailedConnections() {
        return failedConnections.count();
    }

    //    public Timer getConnectionProcessingTimes() {
    //        return connectionProcessingTimes;
    //    }

    public long getPendingWrites() {
        return pendingWritesValue.get();
    }

    public long getPendingFlushes() {
        return pendingFlushesValue.get();
    }

    public double getBytesWritten() {
        return bytesWritten.count();
    }

    //    public Timer getWriteTimes() {
    //        return writeTimes;
    //    }

    public double getBytesRead() {
        return bytesRead.count();
    }

    public double getFailedWrites() {
        return failedWrites.count();
    }

    public double getFailedFlushes() {
        return failedFlushes.count();
    }

    //    public Timer getFlushTimes() {
    //        return flushTimes;
    //    }
}
