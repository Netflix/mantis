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
package io.mantisrx.server.master.store;

import static junit.framework.TestCase.assertEquals;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

@SuppressWarnings("deprecation")
public class DynamoStoreTest {
    public static final String TABLE = "table";
    public static final String V5 = "value5";
    public static final String V2 = "value2";
    public static final String V1 = "value1";
    public static final String V4 = "value4";

    private static final AwsBasicCredentials credentials = AwsBasicCredentials.create("fakeAccessKeyId", "fakeSecretAccessKey");

    private static DynamoDBProxyServer server;
    private static DynamoDbClient client;

	private static String getAvailablePort() {
        try (final ServerSocket serverSocket = new ServerSocket(0)) {
            return String.valueOf(serverSocket.getLocalPort());
        } catch (IOException e) {
            throw new RuntimeException("Available port was not found", e);
        }
    }

    @BeforeClass
    public static void setupDynamo() throws Exception {
		try {
			System.setProperty("sqlite4java.library.path", "build/libs");
            String port = getAvailablePort();
            String uri = "http://localhost:" + port;
            // Create an in-memory and in-process instance of DynamoDB Local that runs over HTTP
            final String[] localArgs = {"-inMemory", "-port", port};
            System.out.println("Starting DynamoDB Local...");
            server = ServerRunner.createServerFromCommandLineArgs(localArgs);
            server.start();

        client = DynamoDbClient.builder()
			.region(Region.US_WEST_2)
			.credentialsProvider(StaticCredentialsProvider.create(credentials))
			.endpointOverride(URI.create("http://localhost:" + port))
			.build();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName(DynamoStore.PK)
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName(DynamoStore.SK)
                                .attributeType(ScalarAttributeType.S)
                                .build()
                )
                .keySchema(
                        KeySchemaElement.builder()
                                .attributeName(DynamoStore.PK)
                                .keyType(KeyType.HASH)
                                .build(),
                        KeySchemaElement.builder()
                                .attributeName(DynamoStore.SK)
                                .keyType(KeyType.RANGE)
                                .build()
                )
                .provisionedThroughput(
                        ProvisionedThroughput.builder()
                                .readCapacityUnits(10L)
                                .writeCapacityUnits(10L)
                                .build())
                .tableName(TABLE)
                .build();

        client.createTable(request);

        final UpdateTimeToLiveRequest ttlRequest = UpdateTimeToLiveRequest.builder()
                .tableName(TABLE)
                .timeToLiveSpecification(
                        TimeToLiveSpecification.builder()
                                .attributeName("expiresAt")
                                .enabled(true)
                                .build())
                .build();

        client.updateTimeToLive(ttlRequest);
    }

    @AfterClass
    public static void TearDown() throws Exception {
        DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
                .tableName(TABLE)
                .build();

        client.deleteTable(deleteTableRequest);
        client.close();
        server.stop();
    }

    @Test
    public void testUpsertOrdered() throws Exception {
        IKeyValueStore store = new DynamoStore(client, TABLE);
        final String pk1 = UUID.randomUUID().toString();
        store.upsertOrdered(TABLE, pk1, 1L, V1, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 2L, V2, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 5L, V5, Duration.ZERO);
        store.upsertOrdered(TABLE, pk1, 4L, V4, Duration.ZERO);
        assertEquals(ImmutableMap.<Long, String>of(5L, V5, 4L, V4, 2L, V2, 1L, V1), store.getAllOrdered(TABLE, pk1, 5));
    }

    @Test
    public void testUpsertMoreThan25andGetAllPk() throws Exception {
        IKeyValueStore store = new DynamoStore(client, TABLE);
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
        IKeyValueStore store = new DynamoStore(client, TABLE);
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
        IKeyValueStore store = new DynamoStore(client, TABLE);
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
            skData3.put(String.valueOf(i), V1);
        }
        assertEquals(true, store.upsertAll(TABLE, pks.get(2), skData3));

        assertEquals(true, store.deleteAll(TABLE, pks.get(0)));
        assertEquals(null, store.get(TABLE, pks.get(0), "3"));
    }

    @Test
    public void testInsertAndGetAllMoreThan25() throws Exception {
        IKeyValueStore store = new DynamoStore(client, TABLE);
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
}
