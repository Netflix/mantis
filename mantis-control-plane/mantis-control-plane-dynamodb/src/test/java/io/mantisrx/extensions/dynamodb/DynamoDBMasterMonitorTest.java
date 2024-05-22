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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import rx.observers.TestSubscriber;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBMasterMonitorTest {

    private static final String TABLE_NAME = "mantis-dynamodb-leader-test";

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
    AmazonDynamoDBLockClient lockClient;

    @Rule
    public DynamoDBLockSupportRule lockSupport =
            new DynamoDBLockSupportRule(TABLE_NAME, dynamoDb.getDynamoDbClient());

    @ClassRule
    public static DynamoDBLocalRule dynamoDb = new DynamoDBLocalRule();

    @Before
    public void testBefore() {
        Mockito.reset(lockClient);
        System.setProperty(DynamoDBConfig.DYNAMO_DB_PROPERTIES_KEY, "dynamodb-test.properties");
    }

    @After
    public void testAfter() throws IOException {
        System.clearProperty(DynamoDBConfig.DYNAMO_DB_PROPERTIES_KEY);
    }

    @Test
    public void getCurrentLeader() throws JsonProcessingException, InterruptedException {
        final String lockKey = "mantis-leader";
        final DynamoDBMasterMonitor m = new DynamoDBMasterMonitor();
        TestSubscriber<MasterDescription> testSubscriber = new TestSubscriber<>();
        m.getMasterObservable().subscribe(testSubscriber);
        m.start();
        assertNull(m.getLatestMaster());
        lockSupport.takeLock(lockKey, jsonMapper.writeValueAsBytes(OTHER_MASTER));
        await()
                .atLeast(Duration.ofMillis(100L))
                .atMost(Duration.ofMillis(1000L))
                .untilAsserted(() -> assertEquals(OTHER_MASTER, m.getLatestMaster()));
        lockSupport.releaseLock(lockKey);
        lockSupport.takeLock(lockKey, jsonMapper.writeValueAsBytes(THIS_MASTER));
        await()
                .atLeast(Duration.ofMillis(100L))
                .atMost(Duration.ofMillis(1000L))
                .untilAsserted(() -> assertEquals(m.getLatestMaster(), THIS_MASTER));
        testSubscriber.assertValues(OTHER_MASTER, THIS_MASTER);
        m.shutdown();
    }

    @Test
    public void runShutdown() throws IOException {
        final String key = "dne";
        final DynamoDBMasterMonitor m = new DynamoDBMasterMonitor(
            lockClient, key, Duration.ofSeconds(1),Duration.ofSeconds(1));
        when(lockClient.getLock(key, Optional.empty())).thenReturn(Optional.empty());
        m.start();
        await()
            .atLeast(Duration.ofSeconds(2));
        m.shutdown();
        verify(lockClient, times(1)).close();

    }
}
