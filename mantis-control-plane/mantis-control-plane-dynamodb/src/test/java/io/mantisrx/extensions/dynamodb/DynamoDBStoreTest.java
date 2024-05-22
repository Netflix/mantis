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
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;

import io.mantisrx.server.core.KeyValueStore;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

public class DynamoDBStoreTest {
    public static final String TABLE = "mantis-dynamodb-kv"; // keep this in sync with dynamodb-test.properties
    public static final String V5 = "value5";
    public static final String V2 = "value2";
    public static final String V1 = "value1";
    public static final String V4 = "value4";

    @ClassRule public static DynamoDBLocalRule dynamoDb = new DynamoDBLocalRule();

    private static final DynamoDbClient client = dynamoDb.getDynamoDbClient();

    @Before
    public void createDatabase() {
        System.setProperty(DynamoDBConfig.DYNAMO_DB_PROPERTIES_KEY, "dynamodb-test.properties");
        client.createTable(createTableRequest(TABLE));
        client.updateTimeToLive(updateTimeToLiveRequest(TABLE));
    }
    @After
    public void deleteDatabase() {
        System.clearProperty(DynamoDBConfig.DYNAMO_DB_PROPERTIES_KEY);
        client.deleteTable(DeleteTableRequest.builder().tableName(TABLE).build());
    }

    @Test
    public void testDefaultConstructor() throws IOException {
        KeyValueStore store = new DynamoDBStore();
        final String pk1 = UUID.randomUUID().toString();
        store.upsertOrdered(TABLE, pk1, 1L, V1, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 2L, V2, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 5L, V5, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 4L, V4, Duration.ZERO);
        assertEquals(ImmutableMap.<Long, String>of(5L, V5, 4L, V4, 2L, V2, 1L, V1), store.getAllOrdered(TABLE, pk1, 5));
    }
    @Test
    public void testUpsertOrdered() throws Exception {
        KeyValueStore store = new DynamoDBStore(client, TABLE);
        final String pk1 = UUID.randomUUID().toString();
        store.upsertOrdered(TABLE, pk1, 1L, V1, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 2L, V2, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 5L, V5, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 4L, V4, Duration.ZERO);
        assertEquals(ImmutableMap.<Long, String>of(5L, V5, 4L, V4, 2L, V2, 1L, V1), store.getAllOrdered(TABLE, pk1, 5));
    }

    @Test
    public void testUpsertMoreThan25andGetAllPk() throws Exception {
        KeyValueStore store = new DynamoDBStore(client, TABLE);
        final List<String> pks = new ArrayList<>();
        for(int i = 0; i<3; i++) {
            pks.add(UUID.randomUUID().toString());
        }
        Collections.sort(pks);
        final Map<String, String> skData1 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData1.put( String.valueOf(i), V1);
        }
        store.upsertAll(TABLE, pks.get(0), skData1);
        final Map<String, String> skData2 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData2.put( String.valueOf(i), V1);
        }
        store.upsertAll(TABLE, pks.get(1), skData2);
        final Map<String, String> skData3 = new HashMap<>();
        for(int i=0; i<7; i++) {
            skData2.put( String.valueOf(i), V1);
        }
        store.upsertAll(TABLE, pks.get(2), skData2);
        final List<String> allPKs = store.getAllPartitionKeys(TABLE);
        Collections.sort(allPKs);
        assertEquals( pks,allPKs);
    }

    @Test
    public void testInsertAndDelete() throws Exception {
        KeyValueStore store = new DynamoDBStore(client, TABLE);
        final String pk1 = UUID.randomUUID().toString();
        store.upsertOrdered(TABLE, pk1, 1L, V1, Duration.ZERO);
        final String data = store.get(TABLE, pk1, "1");
        assertEquals(data, V1);
        final boolean deleteResp = store.delete(TABLE, pk1, "1");
        assertEquals(deleteResp, true);
        final String returnData = store.get(TABLE, pk1, "1");
        assertEquals(returnData, null);

    }

    @Test
    public void testInsertAndDeleteMoreThan25() throws Exception {
        KeyValueStore store = new DynamoDBStore(client, TABLE);
        final List<String> pks = makePKs(3);
        final Map<String, String> skData1 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData1.put( String.valueOf(i), V1);
        }
        assertTrue(store.upsertAll(TABLE, pks.get(0), skData1));

        final Map<String, String> skData2 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData2.put( String.valueOf(i), V1);
        }
        assertTrue(store.upsertAll(TABLE, pks.get(1), skData2));

        final Map<String, String> skData3 = new HashMap<>();
        for(int i=0; i<7; i++) {
            skData3.put(String.valueOf(i), V1);
        }
        assertTrue(store.upsertAll(TABLE, pks.get(2), skData3));

        assertTrue(store.deleteAll(TABLE, pks.get(0)));
        assertNull(store.get(TABLE, pks.get(0), "3"));
    }

    @Test
    public void testInsertAndGetAllMoreThan25() throws Exception {
        KeyValueStore store = new DynamoDBStore(client, TABLE);
        final List<String> pks = makePKs(3);
        final Map<String, String> skData1 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData1.put( String.valueOf(i), V1);
        }
        assertEquals(true,store.upsertAll(TABLE, pks.get(0), skData1));
        final Map<String, String> skData2 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData2.put( String.valueOf(i), V1);
        }
        assertEquals(true,store.upsertAll(TABLE, pks.get(1), skData2));

        final Map<String, String> skData3 = new HashMap<>();
        for(int i=0; i<7; i++) {
            skData3.put( String.valueOf(i), V1);
        }
        assertEquals(true, store.upsertAll(TABLE, pks.get(2), skData3));
        final Map<String, String> itemsPK1 = store.getAll(TABLE, pks.get(0));
        assertEquals(skData1.size(), itemsPK1.size());
        final Map<String, String> itemsPK3 = store.getAll(TABLE, pks.get(2));
        assertEquals(skData3.size(), itemsPK3.size());

    }
    private List<String> makePKs(int num) {
        final List<String> pks = new ArrayList<>();
        for(int i = 0; i<3; i++) {
            pks.add(UUID.randomUUID().toString());
        }
        Collections.sort(pks);
        return pks;
    }

    private CreateTableRequest createTableRequest(String tableName) {
        return CreateTableRequest.builder()
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(DynamoDBStore.PK)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(DynamoDBStore.SK)
                    .attributeType(ScalarAttributeType.S)
                    .build()
            )
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(DynamoDBStore.PK)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName(DynamoDBStore.SK)
                    .keyType(KeyType.RANGE)
                    .build()
            )
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .tableName(tableName)
            .build();
    }

    private UpdateTimeToLiveRequest updateTimeToLiveRequest(String tableName) {
        return UpdateTimeToLiveRequest.builder()
            .tableName(tableName)
            .timeToLiveSpecification(
                TimeToLiveSpecification.builder()
                    .attributeName("expiresAt")
                    .enabled(true)
                    .build()).build();
    }
}
