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

import static io.mantisrx.extensions.dynamodb.DynamoDBMasterMonitor.MASTER_NULL;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import rx.Observable;
import rx.observers.TestSubscriber;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBMasterMonitorTest {

    private static final String TABLE_NAME = "mantis-dynamodb-leader-test";

    private static final Duration GRACEFUL = Duration.ofSeconds(1L);

    private MasterDescription otherMaster;
    private MasterDescription thatMaster;

    @Mock
    AmazonDynamoDBLockClient mockLockClient;

    @Rule
    public DynamoDBLockSupportRule lockSupport =
            new DynamoDBLockSupportRule(TABLE_NAME, dynamoDb.getDynamoDBClient());

    @ClassRule
    public static DynamoDBLocalRule dynamoDb = new DynamoDBLocalRule();

    @Before
    public void testBefore() {
        Mockito.reset(mockLockClient);
        thatMaster = lockSupport.generateDescription();
        otherMaster = lockSupport.generateDescription();
    }

    @After
    public void testAfter() throws IOException {
    }

    @Test
    public void getCurrentLeader() throws JsonProcessingException, InterruptedException {
        final String lockKey = "mantis-leader";
        final DynamoDBMasterMonitor m = new DynamoDBMasterMonitor(
            lockSupport.getLockClient(),
            lockKey,
            DynamoDBLockSupportRule.heartbeatDuration,
            GRACEFUL
            );
        TestSubscriber<MasterDescription> testSubscriber = new TestSubscriber<>();
        m.getMasterObservable().subscribe(testSubscriber);
        m.start();
        assertEquals(MASTER_NULL, m.getLatestMaster());
        lockSupport.takeLock(lockKey, otherMaster);
        await()
                .atLeast(DynamoDBLockSupportRule.heartbeatDuration)
                .pollDelay(DynamoDBLockSupportRule.heartbeatDuration)
                .atMost(Duration.ofMillis(DynamoDBLockSupportRule.heartbeatDuration.toMillis()*2))
                .untilAsserted(() -> assertEquals(otherMaster, m.getLatestMaster()));
        lockSupport.releaseLock(lockKey);
        lockSupport.takeLock(lockKey, thatMaster);
        await()
                .atLeast(DynamoDBLockSupportRule.heartbeatDuration)
                .pollDelay(DynamoDBLockSupportRule.heartbeatDuration)
                .atMost(Duration.ofMillis(DynamoDBLockSupportRule.heartbeatDuration.toMillis()*2))
                .untilAsserted(() -> assertEquals(m.getLatestMaster(), thatMaster));
        testSubscriber.assertValues(MASTER_NULL, otherMaster, thatMaster);
        m.shutdown();
    }

    @Test
    public void runShutdown() throws IOException {
        final String key = "dne";
        final DynamoDBMasterMonitor m = new DynamoDBMasterMonitor(
            mockLockClient,
            key,
            DynamoDBLockSupportRule.heartbeatDuration,
            GRACEFUL
        );
        when(mockLockClient.getLock(key, Optional.empty())).thenReturn(Optional.empty());
        m.start();
        await()
            .atLeast(Duration.ofSeconds(2));
        m.shutdown();
        verify(mockLockClient, times(1)).close();

    }

    @Test
    public void monitorDoesNotReturnNull() throws IOException, InterruptedException {
        final String lockKey = "mantis-leader";
        final DynamoDBMasterMonitor m = new DynamoDBMasterMonitor(
            lockSupport.getLockClient(),
            lockKey,
            DynamoDBLockSupportRule.heartbeatDuration,
            GRACEFUL
        );
        TestSubscriber<MasterDescription> testSubscriber = new TestSubscriber<>();
        m.getMasterObservable().subscribe(testSubscriber);
        m.start();

        // Write Null
        lockSupport.takeLock(lockKey, null);
        await()
            .atLeast(DynamoDBLockSupportRule.heartbeatDuration)
            .pollDelay(DynamoDBLockSupportRule.heartbeatDuration)
            .atMost(Duration.ofMillis(DynamoDBLockSupportRule.heartbeatDuration.toMillis()*2))
            .untilAsserted(() -> assertEquals(MASTER_NULL, m.getLatestMaster()));
        lockSupport.releaseLock(lockKey);

        m.shutdown();

        testSubscriber.assertNoTerminalEvent();
        testSubscriber.assertNotCompleted();
        testSubscriber.assertNoErrors();
        Observable.from(testSubscriber.getOnNextEvents())
            .forEach(Assert::assertNotNull);
    }
}
