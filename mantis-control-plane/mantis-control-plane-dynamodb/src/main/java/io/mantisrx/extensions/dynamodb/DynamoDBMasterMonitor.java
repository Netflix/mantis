package io.mantisrx.extensions.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.ILeadershipManager;
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

    private static final String propertiesFile = "dynamo.properties";
    private static final MasterDescription MASTER_NULL =
            new MasterDescription("NONE", "localhost", -1, -1, -1, "uri://", -1, -1L);
    private final ThreadFactory monitorThreadFactory = r -> {
        Thread thread = new Thread(r);
        thread.setName("master-thread-" + System.currentTimeMillis());
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
     * Creates a MasterMonitor only that can not become a leader.
     *
     * @param leadershipManager the leadershipManager to use
     * @throws IOException the exception is most commonly related to prop file issues
     */
//    public DynamoDBMasterMonitor(ILeadershipManager leadershipManager) throws IOException {
//        this(leadershipManager, false);
//    }

//    public DynamoDBMasterMonitor(ILeadershipManager leadershipManager, boolean canBecomeLeader)
//            throws IOException {
//        this.shouldLeaderElectorBeRunning.set(canBecomeLeader);
//        this.leadershipManager = leadershipManager;
//        this.masterSubject = BehaviorSubject.create();
//
//        String env = StripeEnvironmentUtils.getEnvironment();
//        if (env == null || env.isEmpty()) {
//            throw new IllegalArgumentException("Unable to determine environment (qa/preprod/prod).");
//        }
//
//        Properties props = new Properties();
//        try {
//            props.load(LeaderElectorDynamo.class.getClassLoader().getResourceAsStream(propertiesFile));
//        } catch (IOException e) {
//            logger.error(
//                    SecureMessage.builder()
//                            .message("error loading properties file")
//                            .put("file", Taxonomized.CODE_INTERNALS.forStripe(propertiesFile))
//                            .build(),
//                    e);
//            throw e;
//        }
//        ConfigurationObjectFactory configurationObjectFactory = new ConfigurationObjectFactory(props);
//        DynamoStoreConfiguration conf =
//                configurationObjectFactory.build(DynamoStoreConfiguration.class);
//
//        final DynamoDbClient client = DynamoDbClient.builder().region(Region.of(region)).build();
//        final String lockTable = conf.getDynamoStoreTablePrefix() + "-" + env + "-" + region;
//        this.partitionKey = conf.getDynamoLeaderPartitionKey();
//
//        this.pollInterval = Duration.parse(conf.getDynamoLeaderHeartbeatDuration());
//        this.gracefulShutdown = Duration.parse(conf.getDynamoLeaderGracefulShutdownDuration());
//        final Duration leaseDuration = Duration.parse(conf.getDynamoLeaderLeaseDuration());
//
//        lockClient =
//                new AmazonDynamoDBLockClient(
//                        AmazonDynamoDBLockClientOptions.builder(client, lockTable)
//                                .withLeaseDuration(leaseDuration.toMillis())
//                                .withHeartbeatPeriod(this.pollInterval.toMillis())
//                                .withCreateHeartbeatBackgroundThread(conf.getDynamoLeaderHeartbeatInBackground())
//                                .withTimeUnit(TimeUnit.MILLISECONDS)
//                                .build());
//    }

    public DynamoDBMasterMonitor(
            AmazonDynamoDBLockClient lockClient,
            String key,
            Duration pollInterval,
            Duration gracefulShutdown) {
        this.masterSubject = BehaviorSubject.create();
        this.lockClient = lockClient;
        this.partitionKey = key;
        this.pollInterval = pollInterval;
        this.gracefulShutdown = gracefulShutdown;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void start() {
        leaderMonitor.scheduleAtFixedRate(
                this::getCurrentLeader, 0, this.pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void shutDown() {
        logger.info("shutting down leader election");
        try {
            lockClient.close();
        } catch (IOException e) {
            logger.error("error timeout waiting on leader election to terminate executor", e);
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
        logger.info("leader election shutdown");
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void getCurrentLeader() {
        logger.info("attempting leader lookup");
        final Optional<LockItem> optionalLock = this.lockClient.getLock(partitionKey, Optional.empty());
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
            this.masterSubject.onNext(nextDescription);
        }
    }

    private MasterDescription bytesToMaster(ByteBuffer data) {
        final byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        try {
            return this.jsonMapper.readValue(bytes, MasterDescription.class);
        } catch (IOException e) {
            logger.error("unable to parse master description bytes: {}", data.toString(), e);
        }
        return MASTER_NULL;
    }

    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return masterSubject;
    }

    /**
     * Returns the latest master if there's one. If there has been no master in recently history, then
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

