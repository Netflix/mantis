package io.mantisrx.extensions.dynamodb;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBLeaderElector {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBLeaderElector.class);

    private final ThreadFactory leaderThreadFactory = r -> {
        Thread thread = new Thread(r);
        thread.setName("master-thread-" + System.currentTimeMillis());
        thread.setDaemon(true); // allow JVM to shutdown if monitor is still running
        thread.setPriority(Thread.NORM_PRIORITY);
        thread.setUncaughtExceptionHandler((t, e) -> logger.error("thread: {} failed with {}", t.getName(), e.getMessage(), e) );
        return thread;
    };
    private final ExecutorService leaderElector =
            Executors.newSingleThreadExecutor(leaderThreadFactory);
    private final AtomicBoolean shouldLeaderElectorBeRunning = new AtomicBoolean(false);
    private final AtomicBoolean isLeaderElectorRunning = new AtomicBoolean(false);
}
