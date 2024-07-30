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

import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
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
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;

public class DynamoDBStoreTest {
    private String table = "store-test";
    public static final String V5 = "value5";
    public static final String V2 = "value2";
    public static final String V1 = "value1";
    public static final String V4 = "value4";

    @ClassRule public static DynamoDBLocalRule dynamoDb = new DynamoDBLocalRule();

    private static final DynamoDbClient client = dynamoDb.getDynamoDBClient();

    @Before
    public void createDatabase() {
        table = UUID.randomUUID().toString();
        dynamoDb.createKVTable(table);
    }
    @After
    public void deleteDatabase() {
        client.deleteTable(DeleteTableRequest.builder().tableName(table).build());
    }

    @Test
    public void testUpsertOrdered() throws Exception {
        IKeyValueStore store = new DynamoDBStore(client, table);
        final String pk1 = UUID.randomUUID().toString();
        store.upsertOrdered(table, pk1, 1L, V1, Duration.ZERO);
        store.upsertOrdered(table, pk1, 2L, V2, Duration.ZERO);
        store.upsertOrdered(table, pk1, 5L, V5, Duration.ZERO);
        store.upsertOrdered(table, pk1, 4L, V4, Duration.ZERO);
        assertEquals(ImmutableMap.<Long, String>of(5L, V5, 4L, V4, 2L, V2, 1L, V1), store.getAllOrdered(table, pk1, 5));
    }

    @Test
    public void testUpsertMoreThan25andGetAllPk() throws Exception {
        IKeyValueStore store = new DynamoDBStore(client, table);
        final List<String> pks = new ArrayList<>();
        for(int i = 0; i<3; i++) {
            pks.add(UUID.randomUUID().toString());
        }
        Collections.sort(pks);
        final Map<String, String> skData1 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData1.put( String.valueOf(i), V1);
        }
        store.upsertAll(table, pks.get(0), skData1);
        final Map<String, String> skData2 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData2.put( String.valueOf(i), V1);
        }
        store.upsertAll(table, pks.get(1), skData2);
        final Map<String, String> skData3 = new HashMap<>();
        for(int i=0; i<7; i++) {
            skData2.put( String.valueOf(i), V1);
        }
        store.upsertAll(table, pks.get(2), skData2);
        final List<String> allPKs = store.getAllPartitionKeys(table);
        Collections.sort(allPKs);
        assertEquals( pks,allPKs);
    }

    @Test
    public void testInsertAndDelete() throws Exception {
        IKeyValueStore store = new DynamoDBStore(client, table);
        final String pk1 = UUID.randomUUID().toString();
        store.upsertOrdered(table, pk1, 1L, V1, Duration.ZERO);
        final String data = store.get(table, pk1, "1");
        assertEquals(data, V1);
        final boolean deleteResp = store.delete(table, pk1, "1");
        assertEquals(deleteResp, true);
        final String returnData = store.get(table, pk1, "1");
        assertEquals(returnData, null);

    }

    @Test
    public void testInsertAndDeleteMoreThan25() throws Exception {
        IKeyValueStore store = new DynamoDBStore(client, table);
        final List<String> pks = makePKs(3);
        final Map<String, String> skData1 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData1.put( String.valueOf(i), V1);
        }
        assertTrue(store.upsertAll(table, pks.get(0), skData1));

        final Map<String, String> skData2 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData2.put( String.valueOf(i), V1);
        }
        assertTrue(store.upsertAll(table, pks.get(1), skData2));

        final Map<String, String> skData3 = new HashMap<>();
        for(int i=0; i<7; i++) {
            skData3.put(String.valueOf(i), V1);
        }
        assertTrue(store.upsertAll(table, pks.get(2), skData3));

        assertTrue(store.deleteAll(table, pks.get(0)));
        assertNull(store.get(table, pks.get(0), "3"));
    }

    @Test
    public void testInsertAndGetAllMoreThan25() throws Exception {
        IKeyValueStore store = new DynamoDBStore(client, table);
        final List<String> pks = makePKs(3);
        final Map<String, String> skData1 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData1.put( String.valueOf(i), V1);
        }
        assertEquals(true,store.upsertAll(table, pks.get(0), skData1));
        final Map<String, String> skData2 = new HashMap<>();
        for(int i=0; i<30; i++) {
            skData2.put( String.valueOf(i), V1);
        }
        assertEquals(true,store.upsertAll(table, pks.get(1), skData2));

        final Map<String, String> skData3 = new HashMap<>();
        for(int i=0; i<7; i++) {
            skData3.put( String.valueOf(i), V1);
        }
        assertEquals(true, store.upsertAll(table, pks.get(2), skData3));
        final Map<String, String> itemsPK1 = store.getAll(table, pks.get(0));
        assertEquals(skData1.size(), itemsPK1.size());
        final Map<String, String> itemsPK3 = store.getAll(table, pks.get(2));
        assertEquals(skData3.size(), itemsPK3.size());
    }

    @Test
    public void testInsertAndGetAllMoreThanLimit() throws Exception {
        IKeyValueStore store = new DynamoDBStore(client, table);
        int numRows = DynamoDBStore.QUERY_LIMIT * 2 + 1;
        final List<String> pks = makePKs(1);
        final Map<String, String> skData1 = new HashMap<>();
        for (int i = 0; i < numRows; i++) {
            skData1.put(String.valueOf(i), V1);
        }
        assertTrue(store.upsertAll(table, pks.get(0), skData1));
        final Map<String, String> itemsPK1 = store.getAll(table, pks.get(0));
        assertEquals(skData1.size(), itemsPK1.size());
    }

    @Test
    public void testUpsertAndGetAllPkMoreThanLimit() throws Exception {
        IKeyValueStore store = new DynamoDBStore(client, table);
        int numRows = DynamoDBStore.QUERY_LIMIT * 2 + 1;
        final List<String> pks = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            pks.add(UUID.randomUUID().toString());
        }
        Collections.sort(pks);
        final Map<String, String> skData1 = new HashMap<>();
        for(int i=0; i< numRows; i++) {
            store.upsert(table, pks.get(i), String.valueOf(i), V1);
        }
        final List<String> allPKs = store.getAllPartitionKeys(table);
        Collections.sort(allPKs);
        assertEquals(pks,allPKs);
    }

    private List<String> makePKs(int num) {
        final List<String> pks = new ArrayList<>();
        for(int i = 0; i<3; i++) {
            pks.add(UUID.randomUUID().toString());
        }
        Collections.sort(pks);
        return pks;
    }
}
