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

import static junit.framework.TestCase.assertEquals;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.mantisrx.server.core.BaseService;
import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.server.core.ILeadershipManager;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import rx.observers.TestSubscriber;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBClientSingletonTest {

    private static final String LEADER_TABLE_NAME = "test-mantis-leader-lock-table";
    public static final String V5 = "value5";
    public static final String V2 = "value2";
    public static final String V1 = "value1";
    public static final String V4 = "value4";

    // Generally the use of global state like System Properties is bad practice.
    // I am taking an exception to that here for the benefit of unit testing configuration
    // code that is based on older versions of the library.
    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    @ClassRule
    public static DynamoDBLocalRule dynamoDb = new DynamoDBLocalRule();

    @Rule
    public DynamoDBLockSupportRule lockSupport =
        new DynamoDBLockSupportRule(LEADER_TABLE_NAME, dynamoDb.getDynamoDBClient());

    @Mock
    ILeadershipManager mockLeadershipManager;

    @Before
    public void setup() {
        Mockito.reset(mockLeadershipManager);
    }

    /**
     * The use of global state here and the fact the helper rules all start and stop servers around
     * the tests creates a unique race condition on the values in the java system variables. To
     * reduce that race condition this will use a single test.
     *
     * @throws InterruptedException for thread stop and shutdown
     * @throws IOException for store access issues
     */
    @Test
    public void highAvailabilityServices() throws InterruptedException, IOException {
        final String lockKey = "test-mantis-leader";
        final String table = "local-kv-test";
        final Map<String, String> map = new HashMap<String, String>() {{
            put("mantis.ext.dynamodb.leader.table", LEADER_TABLE_NAME);
            put("mantis.ext.dynamodb.store.table", table);
            put(DynamoDBClientSingleton.DYNAMO_DB_PROPERTIES_KEY, "dynamodb-test.properties");
            put("mantis.ext.dynamodb.leader.key", lockKey);
            put("mantis.ext.dynamodb.endpointOverride", String.format("http://localhost:%d",dynamoDb.getDynamoDBLocalPort()));
        }};
       setSystemProperties(map);

       final DynamoDBLeadershipFactory factory = new DynamoDBLeadershipFactory();
       final BaseService leaderElector= factory.createLeaderElector(null, mockLeadershipManager);
       final MasterMonitor monitor = factory.createLeaderMonitor(null);
       final MasterDescription[] leaders = {
            lockSupport.generateDescription(),
            lockSupport.generateDescription(),
            lockSupport.generateDescription(),
       };
       when(mockLeadershipManager.getDescription()).thenReturn(leaders[0]);
       TestSubscriber<MasterDescription> testSubscriber = new TestSubscriber<>();
       monitor.getMasterObservable().subscribe(testSubscriber);
       monitor.start();
       leaderElector.start();
       await()
            .atMost(Duration.ofSeconds(3L))
            .pollDelay(Duration.ofSeconds(1L))
            .untilAsserted(() -> assertEquals(leaders[0], monitor.getLatestMaster()));
       when(mockLeadershipManager.isLeader()).thenReturn(true);
       leaderElector.shutdown();
       verify(mockLeadershipManager, times(1)).stopBeingLeader();
       lockSupport.takeLock(lockKey, leaders[1]);
        await()
            .atMost(Duration.ofSeconds(3L))
            .pollDelay(Duration.ofSeconds(1L))
            .untilAsserted(() -> assertEquals(leaders[1], monitor.getLatestMaster()));
        lockSupport.releaseLock(lockKey);
        lockSupport.takeLock(lockKey, leaders[2]);
        await()
            .atMost(Duration.ofSeconds(3L))
            .pollDelay(Duration.ofSeconds(1L))
            .untilAsserted(() -> assertEquals(leaders[2], monitor.getLatestMaster()));

        // We can, depending on timing, sometimes get a MASTER_NULL value which is safe to ignore.
        MasterDescription[] actualLeaders = testSubscriber.getOnNextEvents().stream()
            .filter(md -> md != MasterDescription.MASTER_NULL)
            .collect(Collectors.toList())
            .toArray(new MasterDescription[]{});

        assertEquals(leaders.length, actualLeaders.length);
        assertEquals(leaders[0], actualLeaders[0]);
        assertEquals(leaders[1], actualLeaders[1]);
        assertEquals(leaders[2], actualLeaders[2]);
        monitor.shutdown();

        dynamoDb.createKVTable(table);
        IKeyValueStore store = new DynamoDBStore();
        final String pk1 = UUID.randomUUID().toString();
        store.upsertOrdered(table, pk1, 1L, V1, Duration.ZERO);
        store.upsertOrdered(table, pk1, 2L, V2, Duration.ZERO);
        store.upsertOrdered(table, pk1, 5L, V5, Duration.ZERO);
        store.upsertOrdered(table, pk1, 4L, V4, Duration.ZERO);
        assertEquals(ImmutableMap.of(5L, V5, 4L, V4, 2L, V2, 1L, V1), store.getAllOrdered(table, pk1, 5));
        dynamoDb.getDynamoDBClient().deleteTable(DeleteTableRequest.builder().tableName(table).build());
    }
    private void setSystemProperties(Map<String, String> propMap) {
        propMap.forEach((k, v) -> {
            assertNull(System.getProperty(k));
            System.setProperty(k,v);
        });
    }
}
