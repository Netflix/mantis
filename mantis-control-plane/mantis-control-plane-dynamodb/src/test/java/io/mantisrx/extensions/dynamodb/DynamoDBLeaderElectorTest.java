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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.awaitility.core.ConditionFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBLeaderElectorTest {

    private static final String TABLE_NAME = "mantis-dynamodb-leader-test";

    private MasterDescription otherMaster;
    private MasterDescription thisMaster;

    private final ObjectMapper jsonMapper = DefaultObjectMapper.getInstance();

    @Mock
    AmazonDynamoDBLockClient mockLockClient;
    @Mock ILeadershipManager mockLeadershipManager;

    @Rule
    public DynamoDBLockSupportRule lockSupport =
            new DynamoDBLockSupportRule(TABLE_NAME, dynamoDb.getDynamoDBClient());

    @ClassRule
    public static DynamoDBLocalRule dynamoDb = new DynamoDBLocalRule();

    @Before
    public void testBefore() {
        Mockito.reset(mockLockClient, mockLeadershipManager);
        thisMaster = lockSupport.generateDescription();
        otherMaster = lockSupport.generateDescription();
        when(mockLeadershipManager.getDescription()).thenReturn(thisMaster);
    }



    @Test
    public void becomesCurrentLeader() {
        final DynamoDBLeaderElector led = new DynamoDBLeaderElector(
                mockLeadershipManager,
                lockSupport.getLockClient(),
                "leader-key"
            );
        led.start();
        awaitHeartbeat().untilAsserted(() -> assertFalse(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, times(1)).becomeLeader();
    }

    @Test
    public void respectsExistingLock() throws JsonProcessingException, InterruptedException {
        final String key = "leader-key-respect";
        final DynamoDBLeaderElector led = new DynamoDBLeaderElector(
            mockLeadershipManager,
            lockSupport.getLockClient(),
            key
        );
        lockSupport.takeLock(key);
        led.start();
        awaitHeartbeat().untilAsserted(() -> assertTrue(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, never()).becomeLeader();
        lockSupport.releaseLock(key);
        awaitLease().untilAsserted(() -> assertFalse(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, times(1)).becomeLeader();
    }
    @Test
    public void restartsOnFailures() throws InterruptedException {
        final String LOCK_KEY = "restartsOnFailure";
        final DynamoDBLeaderElector led =
                new DynamoDBLeaderElector(
                        mockLeadershipManager,
                        mockLockClient,
                        LOCK_KEY);

        when(mockLockClient.tryAcquireLock(any(AcquireLockOptions.class)))
                .thenThrow(new RuntimeException("testing"))
                .thenAnswer(
                    invocation -> {
                        Thread.sleep(1000L);
                        throw new InterruptedException("in sleep");
                    })
                .thenAnswer(
                    invocation -> {
                        Thread.sleep(Long.MAX_VALUE);
                        return null;
                    });
        led.start();
        awaitHeartbeat().untilAsserted(() -> assertTrue(led.isLeaderElectorRunning()));
        awaitHeartbeat().untilAsserted(() -> assertFalse(led.isLeaderElectorRunning()));
        awaitHeartbeat().untilAsserted(() -> assertTrue(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, never()).becomeLeader();
    }

    @Test
    public void shutdownAsLeader() throws InterruptedException {
        final String key = "shutdownAsLeader";
        final DynamoDBLeaderElector led = new DynamoDBLeaderElector(
            mockLeadershipManager,
            lockSupport.getLockClient(),
            key);
        led.start();
        awaitHeartbeat().untilAsserted(() -> assertFalse(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, times(1)).becomeLeader();
        when(mockLeadershipManager.isLeader()).thenReturn(true);
        led.shutdown();
        verify(mockLeadershipManager, times(1)).stopBeingLeader();
        try (LockItem lockItem = lockSupport.takeLock(key, otherMaster)) {
            MasterDescription md = thisMaster;
            if (lockItem.getData().isPresent()) {
                final byte[] bytes = lockItem.getData().get().array();
                md = jsonMapper.readValue(bytes, MasterDescription.class);
            }
            assertEquals(md, otherMaster);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void shutdownNotAsLeader() throws InterruptedException, JsonProcessingException {
        final String key = "shutdown-not-as-leader";
        final DynamoDBLeaderElector led = new DynamoDBLeaderElector(
            mockLeadershipManager,
            lockSupport.getLockClient(),
            key);
        lockSupport.takeLock(key);
        led.start();
        awaitHeartbeat().untilAsserted(() -> assertTrue(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, never()).becomeLeader();
        when(mockLeadershipManager.isLeader()).thenReturn(false);
        led.shutdown();
        verify(mockLeadershipManager, never()).stopBeingLeader();
    }

    private ConditionFactory awaitLease() {
        return await()
            .atLeast(DynamoDBLockSupportRule.leaseDuration)
            .pollDelay(DynamoDBLockSupportRule.leaseDuration)
            .atMost(DynamoDBLockSupportRule.leaseDuration.multipliedBy(2));
    }

    private ConditionFactory awaitHeartbeat() {
        return await()
            .atLeast(DynamoDBLockSupportRule.heartbeatDuration)
            .pollDelay(DynamoDBLockSupportRule.heartbeatDuration)
            .atMost(DynamoDBLockSupportRule.heartbeatDuration.multipliedBy(2));
    }
}
