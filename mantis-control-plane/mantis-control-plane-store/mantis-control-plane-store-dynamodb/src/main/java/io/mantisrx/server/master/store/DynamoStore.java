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

import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * See mantis-control-plane-dynamodb DynamoDBStore
 */
@Slf4j
@Deprecated
public class DynamoStore implements IKeyValueStore {

    public static final String PK = "PK";
    public  static final String SK = "SK";

    public static final String PARTITION_KEY = "partitionKey";
    public static final String SECONDARY_KEY = "secondaryKey";
    public static final String TABLE_NAME_KEY = "tableName";

    public static final String TTL_KEY = "expiresAt";
    public static final String DATA_KEY = "data";

    private static final String PK_E = "#PK";
    private static final String PK_V = ":PK";
    private static final String SK_E = "#SK";
    private static final String SK_V = ":SK";

    private static final String MPK_E = "#MPK";

    private static final int MAX_ITEMS = 25;

    private final String mantisTable;
    private final DynamoDbClient client;
    public DynamoStore() {
        this.client = DynamoDbClient.builder()
                .build();
        // TODO should we just create the table right here?
        this.mantisTable = "mantis-key-value-store";
    }

    public DynamoStore(DynamoDbClient client, String tableName ) {
        this.client = client;
        this.mantisTable = tableName;
    }
    /**
     * Gets all partition keys from the table.
     * This could be beneficial to call instead of getAllRows
     * if the data volume in the table is large and you want
     * to process rows iteratively.
     * <p>
     * It iterates on partitionKey instead of primaryKey to
     * prevent keys from the same partition coming out of order.
     *
     * @param tableName the table to read from
     * @return list of all partition keys
     */
    @Override
    public List<String> getAllPartitionKeys(String tableName) throws IOException {
        Map<String, String> expressionAttributesNames = new HashMap<>();
        expressionAttributesNames.put(PK_E, PK);
        expressionAttributesNames.put(MPK_E, PARTITION_KEY);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(PK_V, AttributeValue.builder().s(tableName).build());

        final QueryRequest request = QueryRequest.builder()
                .tableName(this.mantisTable)
                .keyConditionExpression(String.format("%s = %s", PK_E, PK_V))
                .expressionAttributeNames(expressionAttributesNames)
                .expressionAttributeValues(expressionAttributeValues)
                .projectionExpression(MPK_E)
                .build();

        log.info("querying for all partition keys in table {}", tableName);
        final QueryResponse response = this.client.query(request);
        final Map<String, String> pks = new HashMap<>();
        response.items().forEach(v -> pks.put(v.get(PARTITION_KEY).s(), ""));
        return new ArrayList<>(pks.keySet());
    }

    /**
     * Gets all rows corresponding to partition key
     *
     * @param tableName    the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @return all records corresponding to partitionKey as a map of secondaryKey -> data
     */
    @Override
    public Map<String, String> getAll(String tableName, String partitionKey) throws IOException {

        Map<String, String> expressionAttributesNames = new HashMap<>();
        expressionAttributesNames.put(PK_E, PK);
        expressionAttributesNames.put(SK_E, SK);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(PK_V, AttributeValue.builder().s(tableName).build());
        expressionAttributeValues.put(SK_V, AttributeValue.builder().s(partitionKey).build());

        final QueryRequest request = QueryRequest.builder()
                .tableName(this.mantisTable)
                .keyConditionExpression(String.format("%s = %s and begins_with(%s, %s)", PK_E, PK_V, SK_E, SK_V))
                .expressionAttributeNames(expressionAttributesNames)
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        log.info("querying for all items in partition {} in table {}", partitionKey, tableName);
        final QueryResponse response = this.client.query(request);
        final Map<String, String> items = new HashMap<>();
        response.items()
                .forEach(v -> items.put(v.get(SECONDARY_KEY).s(), v.get(DATA_KEY).s()));
        return items;
    }

    /**
     * Adds all row corresponding to partition key.
     * The rows are passed as a map of secondaryKey -> data
     *
     * @param tableName    the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param all          map of rows
     * @param ttl          ttl for the record in millis (use null or Duration.ZERO for no expiry)
     * @return boolean if the data was saved
     */
    @Override
    public boolean upsertAll(String tableName, String partitionKey, Map<String, String> all, Duration ttl)
            throws IOException {
        final Duration expiresIn = (ttl == null || ttl.isZero()) ? Duration.ZERO : ttl;
        final List<WriteRequest> writeRequests = writeRequestsFrom(tableName, partitionKey,all, expiresIn);
        return doBatchWriteRequest(writeRequests);
    }


    /**
     * Deletes a row corresponding to the primary key (partitionKey, secondaryKey)
     *
     * @param tableName    the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param secondaryKey secondaryKey for the record
     * @return boolean if row was deleted
     */
    @Override
    public boolean delete(String tableName, String partitionKey, String secondaryKey) throws IOException {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(PK, AttributeValue.builder().s(tableName).build());
        expressionAttributeValues.put(SK, AttributeValue.builder().s(partitionKey + "#" + secondaryKey).build());
        final DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(expressionAttributeValues)
                .build();
        final DeleteItemResponse response = this.client.deleteItem(request);
        response.responseMetadata().requestId();
        log.info("deleted item from table [{}], pk[{}], sk[{}] with request ID {}",
                tableName, partitionKey, secondaryKey, response.responseMetadata().requestId());
        return true;
    }

    /**
     * Deletes all rows corresponding to a partition key
     *
     * @param tableName    the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @return boolean if the rows were deleted
     */
    @Override
    public boolean deleteAll(String tableName, String partitionKey) throws IOException {
        Map<String, String> expressionAttributesNames = new HashMap<>();
        expressionAttributesNames.put(PK_E, PK);
        expressionAttributesNames.put(SK_E, SK);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(PK_V, AttributeValue.builder().s(tableName).build());
        expressionAttributeValues.put(SK_V, AttributeValue.builder().s(partitionKey).build());

        final QueryRequest request = QueryRequest.builder()
                .tableName(this.mantisTable)
                .keyConditionExpression(String.format("%s = %s and begins_with(%s, %s)", PK_E, PK_V, SK_E, SK_V))
                .expressionAttributeNames(expressionAttributesNames)
                .expressionAttributeValues(expressionAttributeValues)
//                .projectionExpression(String.format("%s,%s", PK_E, SK_E))
                .build();
        log.info("querying for all items in partition {} in table {}", partitionKey, tableName);
        final QueryResponse response = this.client.query(request);
        final List<WriteRequest> deleteRequests = new ArrayList<>();
        log.info("retrieved {} from {} and {}", response.items().size(), tableName, partitionKey);
        response.items()
                .forEach(v -> deleteRequests.add(WriteRequest.builder().deleteRequest(
                        DeleteRequest.builder()
                        .key(ImmutableMap.<String, AttributeValue>of(
                                PK, AttributeValue.builder().s(v.get(PK).s()).build(),
                                SK, AttributeValue.builder().s(v.get(SK).s()).build())).build()).build()
                ));
        doBatchWriteRequest(deleteRequests);
        log.info("deleted {} from {} and {}", deleteRequests.size(), tableName, partitionKey);
        return true;
    }

    private WriteRequest writeRequestFrom(String tableName, String partitionKey, String secondaryKey, String data, Duration ttl) {
        final Map<String, AttributeValue> items = new HashMap<>();

        items.put(PK, AttributeValue.builder().s(tableName).build());
        items.put(SK, AttributeValue.builder().s(String.format("%s#%s", partitionKey, secondaryKey)).build());
        items.put(DATA_KEY, AttributeValue.builder().s(data).build());
        items.put(PARTITION_KEY, AttributeValue.builder().s(partitionKey).build());
        items.put(SECONDARY_KEY, AttributeValue.builder().s(secondaryKey).build());
        items.put(TABLE_NAME_KEY, AttributeValue.builder().s(tableName).build());
        if (!ttl.isZero()) {
            items.put(TTL_KEY, AttributeValue.builder()
                    .n(String.valueOf((System.currentTimeMillis()/1000L) + ttl.getSeconds())).build());
        }
        return WriteRequest.builder().putRequest(PutRequest.builder().item(items).build()).build();
    }

    private List<WriteRequest> writeRequestsFrom(String tableName, String partitionKey, Map<String,String> mapSKToData, Duration ttl) {
        List<WriteRequest> writeRequests = new ArrayList<>();
        mapSKToData.forEach((key, value) -> writeRequests.add(
                writeRequestFrom(tableName, partitionKey, key, value, ttl)
        ));
        return writeRequests;
    }
    private WriteRequest deleteRequestFrom(String tableName, String partitionKey, String secondaryKey) {
        final Map<String, AttributeValue> items = new HashMap<>();
        items.put(PK, AttributeValue.builder().s(tableName).build());
        items.put(SK, AttributeValue.builder().s(String.format("%s#%s", partitionKey, secondaryKey)).build());

        return WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(items).build()).build();
    }
    private WriteRequest deleteRequestFrom(String dyanmoPK, String dynamoSK) {
        final Map<String, AttributeValue> items = new HashMap<>();
        log.info("preparing to delete pk {} sk {}", dyanmoPK, dynamoSK);
        items.put(PK, AttributeValue.builder().s(dyanmoPK).build());
        items.put(SK, AttributeValue.builder().s(dynamoSK).build());

        return WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(items).build()).build();
    }
    private List<WriteRequest> deleteRequestsFrom(Map<String, String> mapPKSK) {
        List<WriteRequest> writeRequests = new ArrayList<>();
        mapPKSK.forEach((key, value) -> writeRequests.add(
                deleteRequestFrom(key, value)
        ));
        return writeRequests;
    }

    private boolean doBatchWriteRequest(List<WriteRequest> writeRequests) throws IOException {
        for(int i = 0; i < writeRequests.size(); i +=MAX_ITEMS) {
            final List<WriteRequest> writes = writeRequests.subList(i, Integer.min(i+MAX_ITEMS,writeRequests.size()));
            log.info("processing {} items to {}", writes.size(), this.mantisTable);
            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    .requestItems(ImmutableMap.of(this.mantisTable, writes))
                    .build();

            BatchWriteItemResponse batchWriteItemResponse = this.client.batchWriteItem(batchWriteItemRequest);

            while (!batchWriteItemResponse.hasUnprocessedItems()) {
                Map<String, List<WriteRequest>> unprocessedItems = batchWriteItemResponse.unprocessedItems();
                log.warn("handling {} unprocessed items", unprocessedItems.size());

                batchWriteItemResponse = this.client.batchWriteItem(batchWriteItemRequest);
            }
        }
        return true;
    }
}
