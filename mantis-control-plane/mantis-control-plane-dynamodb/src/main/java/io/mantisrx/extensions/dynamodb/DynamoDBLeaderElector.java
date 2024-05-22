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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBLeaderElector extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBLeaderElector.class);

    private final ThreadFactory leaderThreadFactory = r -> {
        Thread thread = new Thread(r);
        thread.setName("dynamodb-leader-" + System.currentTimeMillis());
        thread.setDaemon(true); // allow JVM to shutdown if monitor is still running
        thread.setPriority(Thread.NORM_PRIORITY);
        thread.setUncaughtExceptionHandler((t, e) -> logger.error("thread: {} failed with {}", t.getName(), e.getMessage(), e) );
        return thread;
    };
    private final ScheduledExecutorService leaderElector =
            Executors.newSingleThreadScheduledExecutor(leaderThreadFactory);
    private final AtomicBoolean shouldLeaderElectorBeRunning = new AtomicBoolean(false);

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
        this.shouldLeaderElectorBeRunning.set(true);
        this.leadershipManager = leadershipManager;
        this.lockClient = lockClient;
        this.partitionKey = key;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void start() {
        leaderElector.submit(this::tryToBecomeLeader);
    }

    @Override
    public void shutdown() {
        logger.info("shutting down");
        shouldLeaderElectorBeRunning.set(false);
        if (leaderLock != null) {
            logger.info("releasing lock");
            leaderLock.close();
        }
        try {
            logger.info("closing lock client");
            lockClient.close();
        } catch (IOException e) {
            logger.error("error timeout waiting on leader election to terminate executor", e);
        }
        if (isLeaderElectorRunning.get()) {
            leaderElector.shutdownNow();
        }
        if (leadershipManager.isLeader()) {
            logger.info("calling stopBeingLeader this may call exit");
            // this may call exit and does no shutdown behavior so let's call it last
            leadershipManager.stopBeingLeader();
        }
        logger.info("shutdown complete");
    }

    public boolean isLeaderElectorRunning() {
        logger.info("leader running: {}", isLeaderElectorRunning.get());
        return isLeaderElectorRunning.get();
    }


    /**
     * This function will attempt to become the leader at the heartbeat interval of the lockClient. If
     * it becomes the leader it will update the leader data and the thread will stop running.
     */
    private void tryToBecomeLeader() {
        final MasterDescription me = leadershipManager.getDescription();
        try {
            logger.info("requesting leadership from {}", me.getHostname());
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
            }
        } catch (RuntimeException | InterruptedException | JsonProcessingException e) {
            logger.error("leader elector task has failed it will restart, if this error is frequent there is likely a problem with DynamoDB based leader election", e);
        } finally {
            if (shouldLeaderElectorBeRunning.get()) {
                this.leaderElector.schedule(this::tryToBecomeLeader, 1L, TimeUnit.SECONDS);
            }
            isLeaderElectorRunning.set(false);
            logger.info("finished leadership request, will restart election: {}", shouldLeaderElectorBeRunning.get());
        }
    }
}
