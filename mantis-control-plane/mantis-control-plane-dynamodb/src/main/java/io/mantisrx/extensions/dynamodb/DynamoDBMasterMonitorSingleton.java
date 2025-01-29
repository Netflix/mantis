/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.extensions.dynamodb;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.subjects.BehaviorSubject;

class DynamoDBMasterMonitorSingleton {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBMasterMonitor.class);


    private final ThreadFactory monitorThreadFactory = r -> {
        Thread thread = new Thread(r);
        thread.setName("dynamodb-monitor-" + System.currentTimeMillis());
        thread.setDaemon(true); // allow JVM to shut down if monitor is still running
        thread.setUncaughtExceptionHandler((t, e) -> logger.error("thread: {} failed with {}", t.getName(), e.getMessage(), e) );
        return thread;
    };
    private final ScheduledExecutorService leaderMonitor =
        Executors.newScheduledThreadPool(1, monitorThreadFactory);

    // Assuming your lock client's options are in a variable named options
    private final AmazonDynamoDBLockClient lockClient;

    private final String partitionKey;

    private final Duration gracefulShutdown;

    private final BehaviorSubject<MasterDescription> masterSubject;

    private final ObjectMapper jsonMapper = DefaultObjectMapper.getInstance();

    private final Duration pollInterval;

    private final Counter noLockPresentCounter;
    private final Counter lockDecodeFailedCounter;
    private final Counter nullNextLeaderCounter;
    private final Counter leaderChangedCounter;
    private final Counter refreshedLeaderCounter;

    private static volatile DynamoDBMasterMonitorSingleton instance = null;

    public static synchronized DynamoDBMasterMonitorSingleton getInstance() {
        if (instance == null) {
            instance = new DynamoDBMasterMonitorSingleton();

            // We allow agents to disable the shutdown hook as avoid losing contact with the master prematurely.
            // Master should keep this because it releases the leader lock.
            if (DynamoDBClientSingleton.getDynamoDBConf().getEnableShutdownHook()) {
                Runtime.getRuntime()
                    .addShutdownHook(new Thread(instance::shutdown, "dynamodb-monitor-shutdown-" + instance.hashCode()));
            }
            instance.start();
        }
        return instance;
    }

    /**
     * Creates a MasterMonitor backed by DynamoDB. This should be used if you are using a {@link DynamoDBLeaderElector}
     */
    DynamoDBMasterMonitorSingleton() {
        this(DynamoDBClientSingleton.getLockClient(),
            DynamoDBClientSingleton.getPartitionKey(),
            Duration.parse(DynamoDBClientSingleton.getDynamoDBConf().getDynamoDBLeaderHeartbeatDuration()),
            Duration.parse(DynamoDBClientSingleton.getDynamoDBConf().getDynamoDBMonitorGracefulShutdownDuration()));
    }

    DynamoDBMasterMonitorSingleton(
        AmazonDynamoDBLockClient lockClient,
        String partitionKey,
        Duration pollInterval,
        Duration gracefulShutdown) {
        masterSubject = BehaviorSubject.create(MasterDescription.MASTER_NULL);
        this.lockClient = lockClient;
        this.partitionKey = partitionKey;
        this.pollInterval = pollInterval;
        this.gracefulShutdown = gracefulShutdown;

        Metrics m = new Metrics.Builder()
            .id("DynamoDBMasterMonitor")
            .addCounter("no_lock_present")
            .addCounter("lock_decode_failed")
            .addCounter("null_next_leader")
            .addCounter("refreshed_leader")
            .addCounter("leader_changed")
            .build();
        Metrics metrics = MetricsRegistry.getInstance().registerAndGet(m);

        this.noLockPresentCounter = metrics.getCounter("no_lock_present");
        this.lockDecodeFailedCounter = metrics.getCounter("lock_decode_failed");
        this.nullNextLeaderCounter = metrics.getCounter("null_next_leader");
        this.refreshedLeaderCounter = metrics.getCounter("refreshed_leader");
        this.leaderChangedCounter = metrics.getCounter("leader_changed");
    }

    public void start() {
        logger.info("starting leader monitor");
        leaderMonitor.scheduleAtFixedRate(
                this::getCurrentLeader, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    void shutdown() {
        logger.info("close the lock client");
        try {
            lockClient.close();
        } catch (IOException e) {
            logger.error("error closing the dynamodb lock client", e);
        }

        try {
            final boolean isTerminated =
                leaderMonitor.awaitTermination(gracefulShutdown.toMillis(), TimeUnit.MILLISECONDS);
            if (!isTerminated) {
                leaderMonitor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("error timeout waiting on leader monitor to terminate executor", e);
        }
        masterSubject.onCompleted();
        logger.info("leader monitor shutdown");
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void getCurrentLeader() {
        logger.info("attempting leader lookup");
        final Optional<LockItem> optionalLock = lockClient.getLock(partitionKey, Optional.empty());
        final MasterDescription nextDescription;
        if (optionalLock.isPresent()) {
            final LockItem lock = optionalLock.get();
            nextDescription = lock.getData().map(this::bytesToMaster).orElse(null);
        } else {
            nextDescription = null;
            logger.warn("no leader found");
            this.noLockPresentCounter.increment();
        }

        if (nextDescription != null) {
            updateLeader(nextDescription);
        } else {
            this.nullNextLeaderCounter.increment();
        }
    }

    private void updateLeader(MasterDescription nextDescription) {
        this.refreshedLeaderCounter.increment();
        final MasterDescription prev = Optional.ofNullable(masterSubject.getValue()).orElse(MasterDescription.MASTER_NULL);
        if (!prev.equals(nextDescription)) {
            this.leaderChangedCounter.increment();
            logger.info("leader changer information previous {} and next {}", prev.getHostname(), nextDescription.getHostname());
            masterSubject.onNext(nextDescription);
        }
    }

    private MasterDescription bytesToMaster(ByteBuffer data) {
        // It is possible that the underlying buffer is read more than once,
        // so if the offset of the buffer is at the end, rewind, so we can read it.
        if (!data.hasRemaining()) {
            data.rewind();
        }
        final byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        try {
            return jsonMapper.readValue(bytes, MasterDescription.class);
        } catch (IOException e) {
            logger.error("unable to parse master description bytes: {}", data, e);
            this.lockDecodeFailedCounter.increment();
        }
        return MasterDescription.MASTER_NULL;
    }

    BehaviorSubject<MasterDescription> getMasterSubject() {
        return masterSubject;
    }
}
