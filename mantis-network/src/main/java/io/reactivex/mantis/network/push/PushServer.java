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

package io.reactivex.mantis.network.push;

import static com.mantisrx.common.utils.MantisMetricStringConstants.GROUP_ID_TAG;
import static io.reactivex.mantis.network.push.PushServerSse.CLIENT_ID_TAG_NAME;

import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.compression.CompressionUtils;
import io.mantisrx.common.messages.MantisMetaDroppedMessage;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import io.netty.util.concurrent.GenericFutureListener;
import io.reactivx.mantis.operators.DisableBackPressureOperator;
import io.reactivx.mantis.operators.DropOperator;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import mantis.io.reactivex.netty.channel.DefaultChannelWriter;
import mantis.io.reactivex.netty.server.RxServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;


public abstract class PushServer<T, R> {

    private static final Logger logger = LoggerFactory.getLogger(PushServer.class);
    final byte[] prefix = "data: ".getBytes();
    final byte[] nwnw = "\n\n".getBytes();
    protected int port;
    protected MonitoredQueue<T> outboundBuffer;
    protected ConnectionManager<T> connectionManager;
    private int writeRetryCount;
    private RxServer<?, ?> server;
    private Counter processedWrites;
    private Counter successfulWrites;
    private Counter failedWrites;
    private Gauge batchWriteSize;
    private Set<Future<Void>> consumerThreadFutures = new HashSet<>();
    private Observable<String> serverSignals;
    private String serverName;
    private final int maxNotWritableTimeSec;
    private final ScheduledExecutorService scheduledExecutorService;
    private final MetricsRegistry metricsRegistry;

    public PushServer(final PushTrigger<T> trigger, ServerConfig<T> config,
                      Observable<String> serverSignals) {

        this.serverSignals = serverSignals;
        serverName = config.getName();
        maxNotWritableTimeSec = config.getMaxNotWritableTimeSec();
        metricsRegistry = config.getMetricsRegistry();

        outboundBuffer = new MonitoredQueue<T>(serverName, config.getBufferCapacity(), config.useSpscQueue());
        trigger.setBuffer(outboundBuffer);

        Action0 doOnFirstConnection = new Action0() {
            @Override
            public void call() {
                trigger.start();
            }
        };
        Action0 doOnZeroConnections = new Action0() {
            @Override
            public void call() {
                logger.info("doOnZeroConnections Triggered");
                trigger.stop();
            }
        };

        final String serverNameValue = Optional.ofNullable(serverName).orElse("none");
        final BasicTag idTag = new BasicTag(GROUP_ID_TAG, serverNameValue);
        final MetricGroupId metricsGroup = new MetricGroupId("PushServer", idTag);
        // manager will auto add metrics for connection groups
        connectionManager = new ConnectionManager<T>(metricsRegistry, doOnFirstConnection,
            doOnZeroConnections);


        int numQueueProcessingThreads = config.getNumQueueConsumers();
        MonitoredThreadPool consumerThreads = new MonitoredThreadPool("QueueConsumerPool",
            new ThreadPoolExecutor(numQueueProcessingThreads, numQueueProcessingThreads, 5, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(numQueueProcessingThreads), new NamedThreadFactory("QueueConsumerPool")));

        logger.info("PushServer create consumer threads, use spsc: {}, num threads: {}, buffer capacity: {}, " +
                "chunk size: {}, chunk time ms: {}", config.useSpscQueue(), numQueueProcessingThreads,
            config.getBufferCapacity(), config.getMaxChunkSize(), config.getMaxChunkTimeMSec());
        if (config.useSpscQueue()) {
            consumerThreadFutures.add(consumerThreads.submit(new SingleThreadedChunker<T>(
                config.getChunkProcessor(),
                outboundBuffer,
                config.getMaxChunkSize(),
                config.getMaxChunkTimeMSec(),
                connectionManager
            )));
        } else {

            for (int i = 0; i < numQueueProcessingThreads; i++) {
                consumerThreadFutures.add(consumerThreads.submit(new TimedChunker<T>(
                    outboundBuffer,
                    config.getMaxChunkSize(),
                    config.getMaxChunkTimeMSec(),
                    config.getChunkProcessor(),
                    connectionManager
                )));
            }
        }

        Metrics serverMetrics = new Metrics.Builder()
            .id(metricsGroup)
            .addCounter("numProcessedWrites")
            .addCounter("numSuccessfulWrites")
            .addCounter("numFailedWrites")
            .addGauge(connectionManager.getActiveConnections(metricsGroup))
            .addGauge("batchWriteSize")
            .build();
        successfulWrites = serverMetrics.getCounter("numSuccessfulWrites");
        failedWrites = serverMetrics.getCounter("numFailedWrites");
        batchWriteSize = serverMetrics.getGauge("batchWriteSize");
        processedWrites = serverMetrics.getCounter("numProcessedWrites");

        registerMetrics(metricsRegistry, serverMetrics, consumerThreads.getMetrics(),
            outboundBuffer.getMetrics(), trigger.getMetrics(),
            config.getChunkProcessor().router.getMetrics());

        port = config.getPort();
        writeRetryCount = config.getWriteRetryCount();
        scheduledExecutorService = new ScheduledThreadPoolExecutor(10,
            new ThreadFactoryBuilder().setNameFormat("netty-channel-checker-%d").build());
    }

    private void registerMetrics(MetricsRegistry registry, Metrics serverMetrics,
                                 Metrics consumerPoolMetrics, Metrics queueMetrics,
                                 Metrics pushTriggerMetrics,
                                 Metrics routerMetrics) {

        registry.registerAndGet(serverMetrics);
        registry.registerAndGet(consumerPoolMetrics);
        registry.registerAndGet(queueMetrics);
        registry.registerAndGet(pushTriggerMetrics);
        registry.registerAndGet(routerMetrics);
    }

    protected Observable<Void> manageConnection(final DefaultChannelWriter<R> writer, String host, int port,
                                                String groupId, String slotId, String id, final AtomicLong lastWriteTime, final boolean applicationHeartbeats,
                                                final Subscription heartbeatSubscription, boolean applySampling, long samplingRateMSec,
                                                Func1<T, Boolean> predicate, final Action0 connectionClosedCallback,
                                                final Counter legacyMsgProcessedCounter, final Counter legacyDroppedWrites,
                                                final Action0 connectionSubscribeCallback) {
        return manageConnection(writer, host, port, groupId, slotId, id, lastWriteTime, applicationHeartbeats, heartbeatSubscription,
            applySampling, samplingRateMSec, null, null, predicate, connectionClosedCallback, legacyMsgProcessedCounter, legacyDroppedWrites, connectionSubscribeCallback);
    }

    protected Observable<Void> manageConnection(final DefaultChannelWriter<R> writer, String host, int port,
                                                String groupId, String slotId, String id, final AtomicLong lastWriteTime, final boolean applicationHeartbeats,
                                                final Subscription heartbeatSubscription, boolean applySampling, long samplingRateMSec,
                                                final SerializedSubject<String, String> metaMsgSubject, final Subscription metaMsgSubscription,
                                                Func1<T, Boolean> predicate, final Action0 connectionClosedCallback,
                                                final Counter legacyMsgProcessedCounter, final Counter legacyDroppedWrites,
                                                final Action0 connectionSubscribeCallback) {
        return manageConnectionWithCompression(writer, host, port, groupId, slotId, id, lastWriteTime, applicationHeartbeats, heartbeatSubscription,
            applySampling, samplingRateMSec, null, null, predicate, connectionClosedCallback, legacyMsgProcessedCounter, legacyDroppedWrites, connectionSubscribeCallback, false, false, null, null);

    }

    /**
     * Adding this for backwards compatibility until all SSE producers and consumers can be migrated to using the pipeline
     * Replace with a pipeline configurator in the future
     *
     * @param writer
     * @param host
     * @param port
     * @param groupId
     * @param slotId
     * @param id
     * @param lastWriteTime
     * @param applicationHeartbeats
     * @param heartbeatSubscription
     * @param applySampling
     * @param samplingRateMSec
     * @param metaMsgSubject
     * @param metaMsgSubscription
     * @param predicate
     * @param connectionClosedCallback
     * @param legacyMsgProcessedCounter
     * @param legacyDroppedWrites
     * @param connectionSubscribeCallback
     * @param compressOutput
     *
     * @return
     */

    protected Observable<Void> manageConnectionWithCompression(final DefaultChannelWriter<R> writer, String host, int port,
                                                               String groupId, String slotId, String id, final AtomicLong lastWriteTime, final boolean applicationHeartbeats,
                                                               final Subscription heartbeatSubscription, boolean applySampling, long samplingRateMSec,
                                                               final SerializedSubject<String, String> metaMsgSubject, final Subscription metaMsgSubscription,
                                                               Func1<T, Boolean> predicate, final Action0 connectionClosedCallback,
                                                               final Counter legacyMsgProcessedCounter, final Counter legacyDroppedWrites,
                                                               final Action0 connectionSubscribeCallback, boolean compressOutput, boolean isSSE,
                                                               byte[] delimiter, String availabilityZone) {

        if (id == null || id.isEmpty()) {
            id = host + "_" + port + "_" + System.currentTimeMillis();
        }

        if (slotId == null || slotId.isEmpty()) {
            slotId = id;
        }

        if (groupId == null || groupId.isEmpty()) {
            groupId = id;
        }

        final BasicTag slotIdTag = new BasicTag("slotId", slotId);

        SerializedSubject<List<byte[]>, List<byte[]>> subject
            = new SerializedSubject<>(PublishSubject.<List<byte[]>>create());
        Observable<List<byte[]>> observable = subject.lift(new DropOperator<>("batch_writes", slotIdTag));

        if (applySampling) {
            observable =
                observable
                    .sample(samplingRateMSec, TimeUnit.MILLISECONDS)
                    .map((List<byte[]> list) -> {
                            // get most recent item from sample
                            List<byte[]> singleItem = new LinkedList<>();
                            if (!list.isEmpty()) {
                                singleItem.add(list.get(list.size() - 1));
                            }
                            return singleItem;
                        }
                    );
        }

        final BasicTag clientIdTag = new BasicTag(CLIENT_ID_TAG_NAME, Optional.ofNullable(groupId).orElse("none"));
        Metrics writableMetrics = new Metrics.Builder()
            .id("PushServer", clientIdTag)
            .addCounter("channelWritable")
            .addCounter("channelNotWritable")
            .addCounter("channelNotWritableTimeout")
            .build();
        metricsRegistry.registerAndGet(writableMetrics);
        Counter channelWritableCounter = writableMetrics.getCounter("channelWritable");
        Counter channelNotWritableCounter = writableMetrics.getCounter("channelNotWritable");
        Counter channelNotWritableTimeoutCounter = writableMetrics.getCounter("channelNotWritableTimeout");

        final Future<?> writableCheck;
        AtomicLong lastWritableTS = new AtomicLong(System.currentTimeMillis());
        if (maxNotWritableTimeSec > 0) {
            writableCheck = scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    long currentTime = System.currentTimeMillis();
                    if (writer.getChannel().isWritable()) {
                        channelWritableCounter.increment();
                        lastWritableTS.set(currentTime);
                    } else if (currentTime - lastWritableTS.get() > TimeUnit.SECONDS.toMillis(maxNotWritableTimeSec)) {
                        logger.warn("Closing connection due to channel not writable for more than {} secs", maxNotWritableTimeSec);
                        channelNotWritableTimeoutCounter.increment();
                        try {
                            writer.close();
                        } catch (Throwable ex) {
                            logger.error("Failed to close connection.", ex);
                        }
                    } else {
                        channelNotWritableCounter.increment();
                    }
                },
                0,
                10,
                TimeUnit.SECONDS
            );
        } else {
            writableCheck = null;
        }

        final AsyncConnection<T> connection = new AsyncConnection<T>(host,
            port, id, slotId, groupId, subject, predicate, availabilityZone);

        final Channel channel = writer.getChannel();
        channel.closeFuture().addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Void>>() {
            @Override
            public void operationComplete(io.netty.util.concurrent.Future<Void> future) throws Exception {
                connectionManager.remove(connection);
                connectionCleanup(heartbeatSubscription, connectionClosedCallback, metaMsgSubscription);
                // Callback from the channel is closed, we don't need to check channel status anymore.
                if (writableCheck != null) {
                    writableCheck.cancel(false);
                }
            }
        });

        return
            observable
                .doOnSubscribe(() -> {
                        connectionManager.add(connection);
                        if (connectionSubscribeCallback != null) {
                            connectionSubscribeCallback.call();
                        }
                    }
                ) // per connection buffer
                .lift(new DisableBackPressureOperator<List<byte[]>>())
                .buffer(200, TimeUnit.MILLISECONDS)
                .flatMap((List<List<byte[]>> bufferOfBuffers) -> {
                        if (bufferOfBuffers != null && !bufferOfBuffers.isEmpty()) {
                            ByteBuffer blockBuffer = null;

                            int size = 0;
                            for (List<byte[]> buffer : bufferOfBuffers) {
                                size += buffer.size();
                            }
                            final int batchSize = size;
                            processedWrites.increment(batchSize);
                            if (channel.isActive() && channel.isWritable()) {
                                lastWritableTS.set(System.currentTimeMillis());
                                if (isSSE) {
                                    if (compressOutput) {
                                        boolean useSnappy = true;
                                        byte[] compressedData =  delimiter == null
                                            ? CompressionUtils.compressAndBase64EncodeBytes(bufferOfBuffers, useSnappy)
                                            : CompressionUtils.compressAndBase64EncodeBytes(bufferOfBuffers, useSnappy, delimiter);

                                        blockBuffer = ByteBuffer.allocate(prefix.length + compressedData.length + nwnw.length);
                                        blockBuffer.put(prefix);
                                        blockBuffer.put(compressedData);
                                        blockBuffer.put(nwnw);
                                    } else {
                                        int totalBytes = 0;
                                        for (List<byte[]> buffer : bufferOfBuffers) {

                                            for (byte[] data : buffer) {
                                                totalBytes += (data.length + prefix.length + nwnw.length);
                                            }
                                        }
                                        byte[] block = new byte[totalBytes];
                                        blockBuffer = ByteBuffer.wrap(block);
                                        for (List<byte[]> buffer : bufferOfBuffers) {
                                            for (byte[] data : buffer) {
                                                blockBuffer.put(prefix);
                                                blockBuffer.put(data);
                                                blockBuffer.put(nwnw);
                                            }
                                        }
                                    }
                                } else {
                                    int totalBytes = 0;
                                    for (List<byte[]> buffer : bufferOfBuffers) {

                                        for (byte[] data : buffer) {
                                            totalBytes += (data.length);
                                        }
                                    }
                                    byte[] block = new byte[totalBytes];
                                    blockBuffer = ByteBuffer.wrap(block);
                                    for (List<byte[]> buffer : bufferOfBuffers) {
                                        for (byte[] data : buffer) {
                                            blockBuffer.put(data);
                                        }
                                    }
                                }
                                return
                                    writer
                                        .writeBytesAndFlush(blockBuffer.array())
                                        .retry(writeRetryCount)
                                        .doOnError((Throwable t1) -> failedToWriteBatch(connection, batchSize, legacyDroppedWrites, metaMsgSubject))
                                        .doOnCompleted(() -> {
                                                if (applicationHeartbeats && lastWriteTime != null) {
                                                    lastWriteTime.set(System.currentTimeMillis());
                                                }
                                                if (legacyMsgProcessedCounter != null) {
                                                    legacyMsgProcessedCounter.increment(batchSize);
                                                }
                                                successfulWrites.increment(batchSize);
                                                connectionManager.successfulWrites(connection, batchSize);
                                            }
                                        )
                                        .doOnTerminate(() -> batchWriteSize.set(batchSize));
                            } else {
                                // connection is not active or writable
                                failedToWriteBatch(connection, batchSize, legacyDroppedWrites, metaMsgSubject);
                            }
                        }
                        return Observable.empty();
                    }
                );
    }

    protected void failedToWriteBatch(AsyncConnection<T> connection,
                                      int batchSize, Counter legacyDroppedWrites, SerializedSubject<String, String> metaMsgSubject) {
        if (legacyDroppedWrites != null) {
            legacyDroppedWrites.increment(batchSize);
        }
        if (metaMsgSubject != null) {
            MantisMetaDroppedMessage msg = new MantisMetaDroppedMessage(batchSize, System.currentTimeMillis());
            metaMsgSubject.onNext(msg.toString());
        }
        failedWrites.increment(batchSize);
        connectionManager.failedWrites(connection, batchSize);
    }

    protected void connectionCleanup(Subscription heartbeatSubscription, Action0 connectionClosedCallback, Subscription metaMsgSubscription) {
        if (heartbeatSubscription != null) {
            logger.info("Unsubscribing from heartbeats");
            heartbeatSubscription.unsubscribe();
        }
        if (metaMsgSubscription != null) {
            logger.info("Unsubscribing from metaMsg subject");
            metaMsgSubscription.unsubscribe();
        }

        if (connectionClosedCallback != null) {
            connectionClosedCallback.call();
        }
    }

    public abstract RxServer<?, ?> createServer();

    public void start() {
        server = createServer();
        server.start();
        serverSignals
            .subscribe(
                (String message) -> logger.info("Signal received for server: " + serverName + " signal: " + message),
                (Throwable t) -> logger.info("Signal received for server: " + serverName + " signal: SERVER_ERROR", t),
                () -> logger.info("Signal received for server: " + serverName + " signal: SERVER_COMPLETED")
            );
    }

    public void blockUntilShutdown() {
        // block on server signal until completed
        serverSignals.toBlocking()
            .forEach((String message) -> { /* no op */ });
    }

    public void startAndBlock() {
        server = createServer();
        server.start();
        try {
            serverSignals.toBlocking()
                .forEach((String message) -> logger.info("Signal received for server: " + serverName + " signal: " + message));
        } catch (Throwable t) {
            logger.info("Signal received for server: " + serverName + " signal: SERVER_ERROR", t);
            throw t;
        }
        logger.info("Signal received for server: " + serverName + " signal: SERVER_COMPLETED");
    }

    public void shutdown() {
        for (Future<Void> thread : consumerThreadFutures) {
            thread.cancel(true);
        }
        try {
            server.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
