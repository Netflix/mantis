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

public class DynamoDBLocalRule extends ExternalResource {
    private final DynamoDBLocalServer server;

    private static final String ENDPOINT_NAME = "mantis.ext.dynamodb.endpointOverride";

    public DynamoDBLocalRule() {
        this.server = new DynamoDBLocalServer();
    }

    @Override
    protected void before() {
        System.setProperty(ENDPOINT_NAME, String.format("http://localhost:%d", server.getPort()));
        server.start();
    }

    @Override
    protected void after() {
        System.clearProperty(ENDPOINT_NAME);
        server.stop();
    }

    public DynamoDbClient getDynamoDbClient() {
        return server.getDynamoDbClient();
    }
}
