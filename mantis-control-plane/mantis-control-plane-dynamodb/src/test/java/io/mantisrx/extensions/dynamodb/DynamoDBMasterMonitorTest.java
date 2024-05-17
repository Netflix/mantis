package io.mantisrx.extensions.dynamodb;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class DynamoDBMasterMonitorTest {

    private static final String TABLE_NAME = "Lock_" + DynamoDBMasterMonitor.class.getSimpleName();

    private static final MasterDescription OTHER_MASTER =
            new MasterDescription(
                    "not-me",
                    "192.168.1.1",
                    23773,
                    23774,
                    23775,
                    "http://star.xyz",
                    23776,
                    System.currentTimeMillis());
    private static final MasterDescription THIS_MASTER =
            new MasterDescription(
                    "me",
                    "10.10.1.1",
                    23773,
                    23774,
                    23775,
                    "http://star.xyz",
                    23776,
                    System.currentTimeMillis());

    private final ObjectMapper jsonMapper = DefaultObjectMapper.getInstance();
    private AmazonDynamoDBLockClient lockClient;

    @Rule
    public DynamoDBLockSupportRule lockSupport =
            new DynamoDBLockSupportRule(TABLE_NAME, dynamoDb.getDynamoDbClient());

    @ClassRule
    public static DynamoDBLocalRule dynamoDb = new DynamoDBLocalRule();

    @Before
    public void testBefore() {
        lockClient = new AmazonDynamoDBLockClient(
                AmazonDynamoDBLockClientOptions.builder(dynamoDb.getDynamoDbClient(), TABLE_NAME)
                        .withLeaseDuration(2000L)
                        .withHeartbeatPeriod(500L)
                        .withCreateHeartbeatBackgroundThread(true)
                        .withTimeUnit(MICROSECONDS)
                        .build());
    }

    @After
    public void testAfter() throws IOException {
        lockClient.close();
    }

    @Test
    public void getCurrentLeader() throws JsonProcessingException, InterruptedException {
        final String lockKey = "getCurrentLeader";
        final DynamoDBMasterMonitor m =
                new DynamoDBMasterMonitor(
                        lockClient,
                        lockKey,
                        Duration.ofMillis(500),
                        Duration.ofMillis(1000));
        m.start();
        assertNull(m.getLatestMaster());
        lockSupport.takeLock(lockKey, jsonMapper.writeValueAsBytes(OTHER_MASTER));
        await()
                .atLeast(Duration.ofMillis(100L))
                .atMost(Duration.ofMillis(1000L))
                .untilAsserted(() -> assertEquals(m.getLatestMaster(), OTHER_MASTER));
        lockSupport.releaseLock(lockKey);
        lockSupport.takeLock(lockKey, jsonMapper.writeValueAsBytes(THIS_MASTER));
        await()
                .atLeast(Duration.ofMillis(100L))
                .atMost(Duration.ofMillis(1000L))
                .untilAsserted(() -> assertEquals(m.getLatestMaster(), THIS_MASTER));
    }
}
