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

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamoDBLeaderElector extends BaseService {

    private final ThreadFactory leaderThreadFactory = r -> {
        Thread thread = new Thread(r);
        thread.setName("dynamodb-leader-" + System.currentTimeMillis());
        thread.setDaemon(true); // allow JVM to shutdown if monitor is still running
        thread.setPriority(Thread.NORM_PRIORITY);
        thread.setUncaughtExceptionHandler((t, e) -> log.error("thread: {} failed with {}", t.getName(), e.getMessage(), e) );
        return thread;
    };
    private final ScheduledExecutorService leaderElector =
            Executors.newSingleThreadScheduledExecutor(leaderThreadFactory);
    private final AtomicBoolean shouldLeaderElectorBeRunning = new AtomicBoolean(true);

    private final AtomicBoolean isLeaderElectorRunning = new AtomicBoolean(false);

    private final ObjectMapper jsonMapper = DefaultObjectMapper.getInstance();
    private final ILeadershipManager leadershipManager;
    private final AmazonDynamoDBLockClient lockClient;
    private final String partitionKey;

    @Nullable
    private LockItem leaderLock = null;

    public DynamoDBLeaderElector(ILeadershipManager leadershipManager) {
        this(leadershipManager,
            DynamoDBClientSingleton.getLockClient(),
            DynamoDBClientSingleton.getPartitionKey());

    }
    public DynamoDBLeaderElector(
            ILeadershipManager leadershipManager,
            AmazonDynamoDBLockClient lockClient,
            String key) {
        this.leadershipManager = leadershipManager;
        this.lockClient = lockClient;
        this.partitionKey = key;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void start() {
       if(!isLeaderElectorRunning() && shouldLeaderElectorBeRunning.get()) {
           log.info("starting leader elector");
           leaderElector.submit(this::tryToBecomeLeader);
       }
    }

    @Override
    public void shutdown() {
        log.info("shutting down");
        shouldLeaderElectorBeRunning.set(false);
        if (leaderLock != null) {
            log.info("releasing lock");
            leaderLock.close();
        }
        try {
            log.info("closing lock client");
            lockClient.close();
        } catch (IOException e) {
            log.error("error timeout waiting on leader election to terminate executor", e);
        }
        if (isLeaderElectorRunning.get()) {
            leaderElector.shutdownNow();
        }
        if (leadershipManager.isLeader()) {
            log.info("calling stopBeingLeader this may call exit");
            // this may call exit and does no shutdown behavior so let's call it last
            leadershipManager.stopBeingLeader();
        }
        log.info("shutdown complete");
    }

    public boolean isLeaderElectorRunning() {
        log.info("leader running: {}", isLeaderElectorRunning.get());
        return isLeaderElectorRunning.get();
    }


    /**
     * This function will attempt to become the leader at the heartbeat interval of the lockClient. If
     * it becomes the leader it will update the leader data and the thread will stop running.
     * Returns true if leader election succeeded, false otherwise.
     */
    protected boolean tryToBecomeLeader() {
        final MasterDescription me = leadershipManager.getDescription();
        try {
            log.info("requesting leadership from {}", me.getHostname());
            isLeaderElectorRunning.set(true);
            final Optional<LockItem> optionalLock =
                    lockClient.tryAcquireLock(
                            AcquireLockOptions.builder(this.partitionKey)
                                    .withReplaceData(true)
                                    .withAcquireReleasedLocksConsistently(true)
                                    .withData(ByteBuffer.wrap(jsonMapper.writeValueAsBytes(me)))
                                    .build());
            if (optionalLock.isPresent()) {
                leaderLock = optionalLock.get();
                shouldLeaderElectorBeRunning.set(false);
                leadershipManager.becomeLeader();
                return true;
            }
            return false;
        } catch (RuntimeException | InterruptedException | JsonProcessingException e) {
            log.error("leader elector task has failed it will restart, if this error is frequent there is likely a problem with DynamoDB based leader election", e);
            return false;
        } finally {
            isLeaderElectorRunning.set(false);
            if (shouldLeaderElectorBeRunning.get()) {
                this.leaderElector.schedule(this::tryToBecomeLeader, 1L, TimeUnit.SECONDS);
            }
            log.info("finished leadership request, will restart election: {}", shouldLeaderElectorBeRunning.get());
        }
    }
}
