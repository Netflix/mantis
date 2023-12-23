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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import mantis.io.reactivex.netty.client.ClientMetricsEvent;
import mantis.io.reactivex.netty.metrics.ClientMetricEventsListener;


/**
 * @author Neeraj Joshi
 */
public class TcpClientListener<T extends ClientMetricsEvent<?>> extends ClientMetricEventsListener<T> {

    private final Gauge liveConnections;
    private final AtomicLong liveConnectionsValue = new AtomicLong(0);
    private final Counter connectionCount;
    private final Gauge pendingConnects;
    private final AtomicLong pendingConnectsValue = new AtomicLong(0);
    private final Counter failedConnects;
    // private  Timer connectionTimes;

    private final Gauge pendingConnectionClose;
    private final AtomicLong pendingConnectionCloseValue = new AtomicLong(0);
    private final Counter failedConnectionClose;

    private final Gauge pendingPoolAcquires;
    private final AtomicLong pendingPoolAcquiresValue = new AtomicLong(0);
    private final Counter failedPoolAcquires;
    //private  Timer poolAcquireTimes;

    private final Gauge pendingPoolReleases;
    private final AtomicLong pendingPoolReleasesValue = new AtomicLong(0);
    private final Counter failedPoolReleases;
    //private  Timer poolReleaseTimes;

    private final Counter poolAcquires;
    private final Counter poolEvictions;
    private final Counter poolReuse;
    private final Counter poolReleases;

    private final Gauge pendingWrites;
    private final AtomicLong pendingWritesValue = new AtomicLong(0);
    private final Gauge pendingFlushes;
    private final AtomicLong pendingFlushesValue = new AtomicLong(0);

    private final Counter bytesWritten;
    //private  Timer writeTimes;
    private final Counter bytesRead;
    private final Counter failedWrites;
    private final Counter failedFlushes;
    //private  Timer flushTimes;
    //private final RefCountingMonitor refCounter;
    private final String monitorId;
    private final MeterRegistry micrometerRegistry;

    protected TcpClientListener(String monitorId, MeterRegistry micrometerRegistry) {
        this.micrometerRegistry = micrometerRegistry;
        this.monitorId = monitorId;

        String groupName = "tcpClient" + "-" + monitorId;
        final String idValue = Optional.ofNullable(monitorId).orElse("none");
//        final BasicTag idTag = new BasicTag(GROUP_ID_TAG, idValue);
        final Tags idTag = Tags.of(GROUP_ID_TAG, idValue);
//        Metrics m = new Metrics.Builder()
//                .id("tcpClient", idTag)
//                .addGauge("liveConnections")
//                .addCounter("connectionCount")
//                .addGauge("pendingConnects")
//                .addCounter("failedConnects")
//                .addGauge("pendingConnectionClose")
//                .addCounter("failedConnectionClose")
//                .addGauge("pendingPoolAcquires")
//                .addCounter("failedPoolAcquires")
//                .addGauge("pendingPoolReleases")
//                .addCounter("failedPoolReleases")
//                .addCounter("poolAcquires")
//                .addCounter("poolEvictions")
//                .addCounter("poolReuse")
//                .addCounter("poolReleases")
//                .addGauge("pendingWrites")
//                .addGauge("pendingFlushes")
//                .addCounter("bytesWritten")
//                .addCounter("bytesRead")
//                .addCounter("failedWrites")
//                .addCounter("failedFlushes")
//                .build();
//
//        m = MetricsRegistry.getInstance().registerAndGet(m);
//
//        //refCounter = new RefCountingMonitor(monitorId);
//        liveConnections = m.getGauge("liveConnections");
//        connectionCount = m.getCounter("connectionCount");
//        pendingConnects = m.getGauge("pendingConnects");
//        failedConnects = m.getCounter("failedConnects");
//        //connectionTimes = newTimer("connectionTimes");
//        pendingConnectionClose = m.getGauge("pendingConnectionClose");
//        failedConnectionClose = m.getCounter("failedConnectionClose");
//        pendingPoolAcquires = m.getGauge("pendingPoolAcquires");
//        //poolAcquireTimes = newTimer("poolAcquireTimes");
//        failedPoolAcquires = m.getCounter("failedPoolAcquires");
//        pendingPoolReleases = m.getGauge("pendingPoolReleases");
//        //poolReleaseTimes = newTimer("poolReleaseTimes");
//        failedPoolReleases = m.getCounter("failedPoolReleases");
//        poolAcquires = m.getCounter("poolAcquires");
//        poolEvictions = m.getCounter("poolEvictions");
//        poolReuse = m.getCounter("poolReuse");
//        poolReleases = m.getCounter("poolReleases");
//
//        pendingWrites = m.getGauge("pendingWrites");
//        pendingFlushes = m.getGauge("pendingFlushes");
//
//        bytesWritten = m.getCounter("bytesWritten");
//        //writeTimes = newTimer("writeTimes");
//        bytesRead = m.getCounter("bytesRead");
//        failedWrites = m.getCounter("failedWrites");
//        failedFlushes = m.getCounter("failedFlushes");
//        //flushTimes = newTimer("flushTimes");

        liveConnections = Gauge.builder(groupName + "_liveConnections", liveConnectionsValue::get)
                .tags(idTag)
                .register(micrometerRegistry);

        connectionCount = Counter.builder(groupName + "_connectionCount")
                .tags(idTag)
                .register(micrometerRegistry);

        pendingConnects = Gauge.builder(groupName + "_pendingConnects", pendingConnectsValue::get)
                .tags(idTag)
                .register(micrometerRegistry);

        failedConnects = Counter.builder(groupName + "_failedConnects")
                .tags(idTag)
                .register(micrometerRegistry);

        pendingConnectionClose = Gauge.builder(groupName + "_pendingConnectionClose", pendingConnectionCloseValue::get)
                .tags(idTag)
                .register(micrometerRegistry);

        failedConnectionClose = Counter.builder(groupName + "_failedConnectionClose")
                .tags(idTag)
                .register(micrometerRegistry);

        pendingPoolAcquires = Gauge.builder(groupName + "_pendingPoolAcquires", pendingPoolAcquiresValue::get)
                .tags(idTag)
                .register(micrometerRegistry);

        failedPoolAcquires = Counter.builder(groupName + "_failedPoolAcquires")
                .tags(idTag)
                .register(micrometerRegistry);

        pendingPoolReleases = Gauge.builder(groupName + "_pendingPoolReleases", pendingPoolReleasesValue::get)
                .tags(idTag)
                .register(micrometerRegistry);

        failedPoolReleases = Counter.builder(groupName + "_failedPoolReleases")
                .tags(idTag)
                .register(micrometerRegistry);

        poolAcquires = Counter.builder(groupName + "_poolAcquires")
                .tags(idTag)
                .register(micrometerRegistry);

        poolEvictions = Counter.builder(groupName + "_poolEvictions")
                .tags(idTag)
                .register(micrometerRegistry);

        poolReuse = Counter.builder(groupName + "_poolReuse")
                .tags(idTag)
                .register(micrometerRegistry);

        poolReleases = Counter.builder(groupName + "_poolReleases")
                .tags(idTag)
                .register(micrometerRegistry);

        pendingWrites = Gauge.builder(groupName + "_pendingWrites", pendingWritesValue::get)
                .tags(idTag)
                .register(micrometerRegistry);

        pendingFlushes = Gauge.builder(groupName + "_pendingFlushes", pendingFlushesValue::get)
                .tags(idTag)
                .register(micrometerRegistry);

        bytesWritten = Counter.builder(groupName + "_bytesWritten")
                .tags(idTag)
                .register(micrometerRegistry);

        bytesRead = Counter.builder(groupName + "_bytesRead")
                .tags(idTag)
                .register(micrometerRegistry);

        failedWrites = Counter.builder(groupName + "_failedWrites")
                .tags(idTag)
                .register(micrometerRegistry);

        failedFlushes = Counter.builder(groupName + "_failedFlushes")
                .tags(idTag)
                .register(micrometerRegistry);
    }

    public static TcpClientListener<ClientMetricsEvent<ClientMetricsEvent.EventType>> newListener(String monitorId, MeterRegistry micrometerRegistry) {
        return new TcpClientListener<ClientMetricsEvent<ClientMetricsEvent.EventType>>(monitorId,micrometerRegistry);
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
        //flushTimes.record(duration, timeUnit);
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
    protected void onPoolReleaseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingPoolReleasesValue.decrementAndGet();
        poolReleases.increment();
        failedPoolReleases.increment();
    }

    @Override
    protected void onPoolReleaseSuccess(long duration, TimeUnit timeUnit) {
        pendingPoolReleasesValue.decrementAndGet();
        poolReleases.increment();
        //        poolReleaseTimes.record(duration, timeUnit);
    }

    @Override
    protected void onPoolReleaseStart() {
        pendingPoolReleasesValue.incrementAndGet();
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
        pendingPoolAcquiresValue.decrementAndGet();
        poolAcquires.increment();
        failedPoolAcquires.increment();
    }

    @Override
    protected void onPoolAcquireSuccess(long duration, TimeUnit timeUnit) {
        pendingPoolAcquiresValue.decrementAndGet();
        poolAcquires.increment();
        //        poolAcquireTimes.record(duration, timeUnit);
    }

    @Override
    protected void onPoolAcquireStart() {
        pendingPoolAcquiresValue.incrementAndGet();
    }

    @Override
    protected void onConnectionCloseFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        liveConnectionsValue.decrementAndGet();// Even though the close failed, the connection isn't live.
        pendingConnectionCloseValue.decrementAndGet();
        failedConnectionClose.increment();
    }

    @Override
    protected void onConnectionCloseSuccess(long duration, TimeUnit timeUnit) {
        liveConnectionsValue.decrementAndGet();
        pendingConnectionCloseValue.decrementAndGet();
    }

    @Override
    protected void onConnectionCloseStart() {
        pendingConnectionCloseValue.incrementAndGet();
    }

    @Override
    protected void onConnectFailed(long duration, TimeUnit timeUnit, Throwable throwable) {
        pendingConnectsValue.decrementAndGet();
        failedConnects.increment();
    }

    @Override
    protected void onConnectSuccess(long duration, TimeUnit timeUnit) {
        pendingConnectsValue.decrementAndGet();
        liveConnectionsValue.incrementAndGet();
        connectionCount.increment();
        //    connectionTimes.record(duration, timeUnit);
    }

    @Override
    protected void onConnectStart() {
        pendingConnectsValue.incrementAndGet();
    }

    @Override
    public void onCompleted() {

    }

    @Override
    public void onSubscribe() {


    }
}
