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
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;

@Slf4j
public class DynamoDBMasterMonitor extends BaseService implements MasterMonitor {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBMasterMonitor.class);

    private static final MasterDescription MASTER_NULL =
            new MasterDescription("NONE", "localhost", -1, -1, -1, "uri://", -1, -1L);
    private final ThreadFactory monitorThreadFactory = r -> {
        Thread thread = new Thread(r);
        thread.setName("dynamodb-monitor-" + System.currentTimeMillis());
        thread.setDaemon(true); // allow JVM to shutdown if monitor is still running
        thread.setPriority(Thread.NORM_PRIORITY);
        thread.setUncaughtExceptionHandler((t, e) -> logger.error("thread: {} failed with {}", t.getName(), e.getMessage(), e) );
        return thread;
    };
    private final ScheduledExecutorService leaderMonitor =
            Executors.newScheduledThreadPool(1, monitorThreadFactory);

    // Assuming your lock client's options are in a variable named options
    private final AmazonDynamoDBLockClient lockClient;

    private final String partitionKey;

    private final Duration pollInterval;

    private final Duration gracefulShutdown;

    private final BehaviorSubject<MasterDescription> masterSubject;
    private final AtomicReference<MasterDescription> latestMaster = new AtomicReference<>();

    private final ObjectMapper jsonMapper = DefaultObjectMapper.getInstance();

    /**
     * Creates a MasterMonitor backed by DynamoDB. This should be used if you are using a {@link DynamoDBLeaderElector}
     */
    public DynamoDBMasterMonitor() {
        masterSubject = BehaviorSubject.create();
        final DynamoDBConfig conf = DynamoDBClientSingleton.getDynamoDBConf();
        pollInterval = Duration.parse(conf.getDynamoDBLeaderHeartbeatDuration());
        gracefulShutdown = Duration.parse(conf.getDynamoDBMonitorGracefulShutdownDuration());
        lockClient = DynamoDBClientSingleton.getLockClient();
        partitionKey = DynamoDBClientSingleton.getPartitionKey();
    }

    public DynamoDBMasterMonitor(
            AmazonDynamoDBLockClient lockClient,
            String partitionKey,
            Duration pollInterval,
            Duration gracefulShutdown) {
        masterSubject = BehaviorSubject.create();
        this.lockClient = lockClient;
        this.partitionKey = partitionKey;
        this.pollInterval = pollInterval;
        this.gracefulShutdown = gracefulShutdown;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void start() {
        leaderMonitor.scheduleAtFixedRate(
                this::getCurrentLeader, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
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
        }
        updateLeader(nextDescription);
    }

    private void updateLeader(@Nullable MasterDescription nextDescription) {
        final MasterDescription previousDescription = latestMaster.getAndSet(nextDescription);
        final MasterDescription next = (nextDescription == null) ? MASTER_NULL : nextDescription;
        final MasterDescription prev =
                (previousDescription == null) ? MASTER_NULL : previousDescription;
        if (!prev.equals(next)) {
            logger.info("leader changer information previous {} and next {}", prev.getHostname(), next.getHostname());
            masterSubject.onNext(next);
        }
    }

    private MasterDescription bytesToMaster(ByteBuffer data) {
        final byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        try {
            return jsonMapper.readValue(bytes, MasterDescription.class);
        } catch (IOException e) {
            logger.error("unable to parse master description bytes: {}", data, e);
        }
        return MASTER_NULL;
    }

    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return masterSubject;
    }

    /**
     * Returns the latest master if there's one. If there has been no master in recent history, then
     * this return null.
     *
     * @return Latest description of the master
     */
    @Override
    @Nullable
    public MasterDescription getLatestMaster() {
        return latestMaster.get();
    }
}
