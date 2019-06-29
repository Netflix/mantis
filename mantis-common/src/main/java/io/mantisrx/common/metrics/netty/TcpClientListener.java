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
import io.mantisrx.common.metrics.MetricsRegistry;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.metrics.ClientMetricEventsListener;


/**
 * @author Neeraj Joshi
 */
public class TcpClientListener<T extends ClientMetricsEvent<?>> extends ClientMetricEventsListener<T> {

    private final Gauge liveConnections;
    private final Counter connectionCount;
    private final Gauge pendingConnects;
    private final Counter failedConnects;
    // private  Timer connectionTimes;

    private final Gauge pendingConnectionClose;
    private final Counter failedConnectionClose;

    private final Gauge pendingPoolAcquires;
    private final Counter failedPoolAcquires;
    //private  Timer poolAcquireTimes;

    private final Gauge pendingPoolReleases;
    private final Counter failedPoolReleases;
    //private  Timer poolReleaseTimes;

    private final Counter poolAcquires;
    private final Counter poolEvictions;
    private final Counter poolReuse;
    private final Counter poolReleases;

    private final Gauge pendingWrites;
    private final Gauge pendingFlushes;

    private final Counter bytesWritten;
    //private  Timer writeTimes;
    private final Counter bytesRead;
    private final Counter failedWrites;
    private final Counter failedFlushes;
    //private  Timer flushTimes;
    //private final RefCountingMonitor refCounter;
    private final String monitorId;

    protected TcpClientListener(String monitorId) {

        this.monitorId = monitorId;
        final String idValue = Optional.ofNullable(monitorId).orElse("none");
        final BasicTag idTag = new BasicTag(GROUP_ID_TAG, idValue);
        Metrics m = new Metrics.Builder()
                .id("tcpClient", idTag)
                .addGauge("liveConnections")
                .addCounter("connectionCount")
                .addGauge("pendingConnects")
                .addCounter("failedConnects")
                .addGauge("pendingConnectionClose")
                .addCounter("failedConnectionClose")
                .addGauge("pendingPoolAcquires")
                .addCounter("failedPoolAcquires")
                .addGauge("pendingPoolReleases")
                .addCounter("failedPoolReleases")
                .addCounter("poolAcquires")
                .addCounter("poolEvictions")
                .addCounter("poolReuse")
                .addCounter("poolReleases")
                .addGauge("pendingWrites")
                .addGauge("pendingFlushes")
                .addCounter("bytesWritten")
                .addCounter("bytesRead")
                .addCounter("failedWrites")
                .addCounter("failedFlushes")
                .build();

        m = MetricsRegistry.getInstance().registerAndGet(m);

        //refCounter = new RefCountingMonitor(monitorId);
        liveConnections = m.getGauge("liveConnections");
        connectionCount = m.getCounter("connectionCount");
        pendingConnects = m.getGauge("pendingConnects");
        failedConnects = m.getCounter("failedConnects");
        //connectionTimes = newTimer("connectionTimes");
        pendingConnectionClose = m.getGauge("pendingConnectionClose");
        failedConnectionClose = m.getCounter("failedConnectionClose");
        pendingPoolAcquires = m.getGauge("pendingPoolAcquires");
        //poolAcquireTimes = newTimer("poolAcquireTimes");
        failedPoolAcquires = m.getCounter("failedPoolAcquires");
        pendingPoolReleases = m.getGauge("pendingPoolReleases");
        //poolReleaseTimes = newTimer("poolReleaseTimes");
        failedPoolReleases = m.getCounter("failedPoolReleases");
        poolAcquires = m.getCounter("poolAcquires");
        poolEvictions = m.getCounter("poolEvictions");
        poolReuse = m.getCounter("poolReuse");
        poolReleases = m.getCounter("poolReleases");

        pendingWrites = m.getGauge("pendingWrites");
        pendingFlushes = m.getGauge("pendingFlushes");

        bytesWritten = m.getCounter("bytesWritten");
        //writeTimes = newTimer("writeTimes");
        bytesRead = m.getCounter("bytesRead");
        failedWrites = m.getCounter("failedWrites");
        failedFlushes = m.getCounter("failedFlushes");
        //flushTimes = newTimer("flushTimes");


    }

    public static TcpClientListener<ClientMetricsEvent<ClientMetricsEvent.EventType>> newListener(String monitorId) {
        return new TcpClientListener<ClientMetricsEvent<ClientMetricsEvent.EventType>>(monitorId);
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
        //flushTimes.record(duration, timeUnit);
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
    protected void onPoolReleaseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingPoolReleases.decrement();
        poolReleases.increment();
        failedPoolReleases.increment();
    }

    @Override
    protected void onPoolReleaseSuccess(long duration, TimeUnit timeUnit) {
        pendingPoolReleases.decrement();
        poolReleases.increment();
        //        poolReleaseTimes.record(duration, timeUnit);
    }

    @Override
    protected void onPoolReleaseStart() {
        pendingPoolReleases.increment();
    }

    @Override
    protected void onPooledConnectionEviction() {
        poolEvictions.increment();
    }

    @Override
    protected void onPooledConnectionReuse(long duration, TimeUnit timeUnit) {
        poolReuse.increment();
    }

    @Override
    protected void onPoolAcquireFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingPoolAcquires.decrement();
        poolAcquires.increment();
        failedPoolAcquires.increment();
    }

    @Override
    protected void onPoolAcquireSuccess(long duration, TimeUnit timeUnit) {
        pendingPoolAcquires.decrement();
        poolAcquires.increment();
        //        poolAcquireTimes.record(duration, timeUnit);
    }

    @Override
    protected void onPoolAcquireStart() {
        pendingPoolAcquires.increment();
    }

    @Override
    protected void onConnectionCloseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        liveConnections.decrement(); // Even though the close failed, the connection isn't live.
        pendingConnectionClose.decrement();
        failedConnectionClose.increment();
    }

    @Override
    protected void onConnectionCloseSuccess(long duration, TimeUnit timeUnit) {
        liveConnections.decrement();
        pendingConnectionClose.decrement();
    }

    @Override
    protected void onConnectionCloseStart() {
        pendingConnectionClose.increment();
    }

    @Override
    protected void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingConnects.decrement();
        failedConnects.increment();
    }

    @Override
    protected void onConnectSuccess(long duration, TimeUnit timeUnit) {
        pendingConnects.decrement();
        liveConnections.increment();
        connectionCount.increment();
        //    connectionTimes.record(duration, timeUnit);
    }

    @Override
    protected void onConnectStart() {
        pendingConnects.increment();
    }

    @Override
    public void onCompleted() {

    }

    @Override
    public void onSubscribe() {


    }
}
