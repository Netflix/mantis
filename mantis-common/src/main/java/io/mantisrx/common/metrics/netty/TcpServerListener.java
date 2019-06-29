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

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import mantis.io.reactivex.netty.metrics.ServerMetricEventsListener;
import mantis.io.reactivex.netty.server.ServerMetricsEvent;


/**
 * @author Neeraj Joshi
 */
public class TcpServerListener<T extends ServerMetricsEvent<?>> extends ServerMetricEventsListener<T> {

    private final Gauge liveConnections;
    private final Gauge inflightConnections;
    private final Counter failedConnections;
    //private final Timer connectionProcessingTimes;
    private final Gauge pendingConnectionClose;
    private final Counter failedConnectionClose;
    //private final Timer connectionCloseTimes;

    private final Gauge pendingWrites;
    private final Gauge pendingFlushes;

    private final Counter bytesWritten;
    //private final Timer writeTimes;
    private final Counter bytesRead;
    private final Counter failedWrites;
    private final Counter failedFlushes;
    //private final Timer flushTimes;


    protected TcpServerListener(String monitorId) {

        Metrics m = new Metrics.Builder()
                .name("tcpServer_" + monitorId)
                .addGauge("liveConnections")
                .addGauge("inflightConnections")
                .addCounter("failedConnections")
                .addGauge("pendingConnectionClose")
                .addCounter("failedConnectionClose")
                .addGauge("pendingWrites")
                .addGauge("pendingFlushes")
                .addCounter("bytesWritten")
                .addCounter("bytesRead")
                .addCounter("failedWrites")
                .addCounter("failedFlushes")
                .build();

        m = MetricsRegistry.getInstance().registerAndGet(m);

        liveConnections = m.getGauge("liveConnections");
        inflightConnections = m.getGauge("inflightConnections");
        failedConnections = m.getCounter("failedConnections");
        pendingConnectionClose = m.getGauge("pendingConnectionClose");
        failedConnectionClose = m.getCounter("failedConnectionClose");
        //        connectionProcessingTimes = newTimer("connectionProcessingTimes");
        //        connectionCloseTimes = newTimer("connectionCloseTimes");

        pendingWrites = m.getGauge("pendingWrites");
        pendingFlushes = m.getGauge("pendingFlushes");

        bytesWritten = m.getCounter("bytesWritten");
        //   writeTimes = newTimer("writeTimes");
        bytesRead = m.getCounter("bytesRead");
        failedWrites = m.getCounter("failedWrites");
        failedFlushes = m.getCounter("failedFlushes");
        //    flushTimes = newTimer("flushTimes");
    }

    public static TcpServerListener<ServerMetricsEvent<ServerMetricsEvent.EventType>> newListener(String monitorId) {
        return new TcpServerListener<ServerMetricsEvent<ServerMetricsEvent.EventType>>(monitorId);
    }

    @Override
    protected void onConnectionHandlingFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        inflightConnections.decrement();
        failedConnections.increment();
    }

    @Override
    protected void onConnectionHandlingSuccess(long duration, TimeUnit timeUnit) {
        inflightConnections.decrement();
        //connectionProcessingTimes.record(duration, timeUnit);
    }

    @Override
    protected void onConnectionHandlingStart(long duration, TimeUnit timeUnit) {
        inflightConnections.increment();
    }

    @Override
    protected void onConnectionCloseStart() {
        pendingConnectionClose.increment();
    }

    @Override
    protected void onConnectionCloseSuccess(long duration, TimeUnit timeUnit) {
        liveConnections.decrement();
        pendingConnectionClose.decrement();
        //connectionCloseTimes.record(duration, timeUnit);
    }

    @Override
    protected void onConnectionCloseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        liveConnections.decrement();
        pendingConnectionClose.decrement();
        // connectionCloseTimes.record(duration, timeUnit);
        failedConnectionClose.increment();
    }

    @Override
    protected void onNewClientConnected() {
        liveConnections.increment();
    }

    @Override
    protected void onByteRead(long bytesRead) {
        this.bytesRead.increment(bytesRead);
    }

    @Override
    protected void onFlushFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingFlushes.decrement();
        failedFlushes.increment();
    }

    @Override
    protected void onFlushSuccess(long duration, TimeUnit timeUnit) {
        pendingFlushes.decrement();
        //   flushTimes.record(duration, timeUnit);
    }

    @Override
    protected void onFlushStart() {
        pendingFlushes.increment();
    }

    @Override
    protected void onWriteFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingWrites.decrement();
        failedWrites.increment();
    }

    @Override
    protected void onWriteSuccess(long duration, TimeUnit timeUnit, long bytesWritten) {
        pendingWrites.decrement();
        this.bytesWritten.increment(bytesWritten);
        //writeTimes.record(duration, timeUnit);
    }

    @Override
    protected void onWriteStart() {
        pendingWrites.increment();
    }

    @Override
    public void onCompleted() {

    }

    @Override
    public void onSubscribe() {

    }

    public long getLiveConnections() {
        return liveConnections.value();
    }

    public long getInflightConnections() {
        return inflightConnections.value();
    }

    public long getFailedConnections() {
        return failedConnections.value();
    }

    //    public Timer getConnectionProcessingTimes() {
    //        return connectionProcessingTimes;
    //    }

    public long getPendingWrites() {
        return pendingWrites.value();
    }

    public long getPendingFlushes() {
        return pendingFlushes.value();
    }

    public long getBytesWritten() {
        return bytesWritten.value();
    }

    //    public Timer getWriteTimes() {
    //        return writeTimes;
    //    }

    public long getBytesRead() {
        return bytesRead.value();
    }

    public long getFailedWrites() {
        return failedWrites.value();
    }

    public long getFailedFlushes() {
        return failedFlushes.value();
    }

    //    public Timer getFlushTimes() {
    //        return flushTimes;
    //    }
}
