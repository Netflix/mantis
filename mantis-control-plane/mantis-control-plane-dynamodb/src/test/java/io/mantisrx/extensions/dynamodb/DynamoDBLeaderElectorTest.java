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
import java.time.Duration;
import org.junit.After;
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
    private static final String LOCK_KEY = "mantis-leader";

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

    @Mock
    AmazonDynamoDBLockClient mockLockClient;
    @Mock ILeadershipManager mockLeadershipManager;

    @Rule
    public DynamoDBLockSupportRule lockSupport =
            new DynamoDBLockSupportRule(TABLE_NAME, dynamoDb.getDynamoDbClient());

    @ClassRule
    public static DynamoDBLocalRule dynamoDb = new DynamoDBLocalRule();

    @Before
    public void testBefore() {
        Mockito.reset(mockLockClient, mockLeadershipManager);
        when(mockLeadershipManager.getDescription()).thenReturn(THIS_MASTER);

        System.setProperty(DynamoDBConfig.DYNAMO_DB_PROPERTIES_KEY, "dynamodb-test.properties");
    }

    @After
    public void testAfter() throws IOException {
        System.clearProperty(DynamoDBConfig.DYNAMO_DB_PROPERTIES_KEY);
    }

    @Test
    public void becomesCurrentLeader() {
        final DynamoDBLeaderElector led = new DynamoDBLeaderElector(mockLeadershipManager);
        led.start();
        await()
                .atLeast(Duration.ofMillis(100L))
                .atMost(Duration.ofMillis(600L))
                .untilAsserted(() -> assertFalse(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, times(1)).becomeLeader();
    }

    @Test
    public void respectsExistingLock() throws JsonProcessingException, InterruptedException {
        final DynamoDBLeaderElector led = new DynamoDBLeaderElector(mockLeadershipManager);
        lockSupport.takeLock(LOCK_KEY,jsonMapper.writeValueAsBytes(OTHER_MASTER));
        led.start();
        await()
                .atLeast(Duration.ofSeconds(2))
                .pollDelay(Duration.ofSeconds(2))
                .pollInterval(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(6))
                .untilAsserted(() -> assertTrue(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, never()).becomeLeader();
        lockSupport.releaseLock(LOCK_KEY);
        await()
                .atLeast(Duration.ofSeconds(2))
                .pollDelay(Duration.ofSeconds(2))
                .pollInterval(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(6))
                .untilAsserted(() -> assertFalse(led.isLeaderElectorRunning()));
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
        await()
                .atLeast(Duration.ofMillis(500L))
                .pollInterval(Duration.ofMillis(500L))
                .atMost(Duration.ofMillis(3000L))
                .untilAsserted(() -> assertTrue(led.isLeaderElectorRunning()));
        await()
                .atLeast(Duration.ofMillis(500L))
                .pollInterval(Duration.ofMillis(500L))
                .atMost(Duration.ofMillis(3000L))
                .untilAsserted(() -> assertFalse(led.isLeaderElectorRunning()));
        await()
                .atLeast(Duration.ofMillis(500L))
                .pollInterval(Duration.ofMillis(500L))
                .atMost(Duration.ofMillis(3000L))
                .untilAsserted(() -> assertTrue(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, never()).becomeLeader();
    }

    @Test
    public void shutdownAsLeader() throws InterruptedException {
        final String LOCK_KEY = "shutdownAsLeader";
        final DynamoDBLeaderElector led = new DynamoDBLeaderElector(mockLeadershipManager);
        led.start();
        await()
                .atLeast(Duration.ofMillis(200L))
                .atMost(Duration.ofMillis(1000L))
                .untilAsserted(() -> assertFalse(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, times(1)).becomeLeader();
        when(mockLeadershipManager.isLeader()).thenReturn(true);
        led.shutdown();
        verify(mockLeadershipManager, times(1)).stopBeingLeader();
        try (LockItem lockItem = lockSupport.takeLock(LOCK_KEY, jsonMapper.writeValueAsBytes(OTHER_MASTER))) {
            MasterDescription md = THIS_MASTER;
            if (lockItem.getData().isPresent()) {
                final byte[] bytes = lockItem.getData().get().array();
                md = jsonMapper.readValue(bytes, MasterDescription.class);
            }
            assertEquals(md, OTHER_MASTER);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void shutdownNotAsLeader() throws InterruptedException, JsonProcessingException {
        final DynamoDBLeaderElector led = new DynamoDBLeaderElector(mockLeadershipManager);
        lockSupport.takeLock(LOCK_KEY, jsonMapper.writeValueAsBytes(OTHER_MASTER));
        led.start();
        await()
                .pollDelay(Duration.ofMillis(500L))
                .atMost(Duration.ofMillis(1000L))
                .untilAsserted(() -> assertTrue(led.isLeaderElectorRunning()));
        verify(mockLeadershipManager, never()).becomeLeader();
        when(mockLeadershipManager.isLeader()).thenReturn(false);
        led.shutdown();
        verify(mockLeadershipManager, never()).stopBeingLeader();
    }
}
