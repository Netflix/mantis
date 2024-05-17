package io.mantisrx.extensions.dynamodb;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class DynamoDBLeaderElectorTest {

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

    class MockLeadershipManager implements ILeadershipManager{

        private int becomeLeaderCount = 0;
        private int stopBeingLeaderCount = 0;
        private boolean isLeader = false;

        public int getBecomeLeaderCount() {
            return becomeLeaderCount;
        }

        public int getStopBeingLeaderCount() {
            return stopBeingLeaderCount;
        }

        @Override
        public void becomeLeader() {
            this.becomeLeaderCount++;
            this.isLeader = true;
        }

        @Override
        public void stopBeingLeader() {
            this.stopBeingLeaderCount++;
            this.isLeader = false;
        }

        @Override
        public boolean isLeader() {
            return this.isLeader;
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setLeaderReady() {

        }

        @Override
        public MasterDescription getDescription() {
            return THIS_MASTER;
        }
    }
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
    public void becomesCurrentLeader() {
        final String lockKey = "becomesCurrentLeader";
        final MockLeadershipManager leadershipManager = new MockLeadershipManager();
        final DynamoDBLeaderElector led =
                new DynamoDBLeaderElector(
                        leadershipManager,
                        lockClient,
                        lockKey);
        led.start();
        await()
                .atLeast(Duration.ofMillis(100L))
                .atMost(Duration.ofMillis(600L))
                .untilAsserted(() -> assertEquals(leadershipManager.becomeLeaderCount, 1));
    }
}
