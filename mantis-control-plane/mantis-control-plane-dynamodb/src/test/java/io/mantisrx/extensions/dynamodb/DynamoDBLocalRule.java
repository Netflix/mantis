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

import org.junit.rules.ExternalResource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

public class DynamoDBLocalRule extends ExternalResource {
    private final DynamoDBLocalServer server;


    public DynamoDBLocalRule() {
        this.server = new DynamoDBLocalServer();
    }

    @Override
    protected void before() {
        server.start();
    }

    @Override
    protected void after() {
        server.stop();
    }

    public DynamoDbClient getDynamoDBClient() {
        return server.getDynamoDbClient();
    }

    public int getDynamoDBLocalPort() {
        return server.getPort();
    }

    public void createKVTable(String tableName) {
        server.getDynamoDbClient().createTable(createKVTableRequest(tableName));
        server.getDynamoDbClient().updateTimeToLive(updateTimeToLiveRequest(tableName, "expiresAt"));
    }

    private CreateTableRequest createKVTableRequest(String tableName) {
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

    public UpdateTimeToLiveRequest updateTimeToLiveRequest(String tableName, String attributeName) {
        return UpdateTimeToLiveRequest.builder()
            .tableName(tableName)
            .timeToLiveSpecification(
                TimeToLiveSpecification.builder()
                    .attributeName(attributeName)
                    .enabled(true)
                    .build()).build();
    }
}
