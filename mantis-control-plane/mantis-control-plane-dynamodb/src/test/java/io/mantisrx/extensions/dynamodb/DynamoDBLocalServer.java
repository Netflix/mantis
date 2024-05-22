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

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * LocalDynamoDbServer is used to run a local DynamoDb server in tests and local development. It
 * wraps a DynamoDBProxyServer and adds some conveniences like finding an open port and exposing a
 * ready to use client.
 */
public class DynamoDBLocalServer {
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBLocalServer.class);


    private static final AwsBasicCredentials credentials = AwsBasicCredentials.create("fakeAccessKeyId", "fakeSecretAccessKey");
    @Getter
    private final int port;
    private final DynamoDBProxyServer server;
    private final DynamoDbClient client;
    private boolean started = false;

    public DynamoDBLocalServer() {
        System.setProperty("sqlite4java.library.path", "build/libs");
        this.port = getFreePort();
        this.server = createServer(this.port);
        this.client = createClient(this.port);
    }

    public void start() {
        if (started) {
            logger.warn("attempted to start dynamodb local more than once on port: {}", this.port);
            return;
        }
        try {
            logger.info("starting dynamodb local on port: {}", this.port);
            server.start();
            started = true;
        } catch (Exception e) {
            logger.error("failed to start dynamodb local", e);
            throw new IllegalStateException(e);
        }
    }

    public void stop() {
        stopUnchecked();
    }

    public DynamoDbClient getDynamoDbClient() {
        return client;
    }

    private void stopUnchecked() {
        try {
            server.stop();
            started = false;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private DynamoDBProxyServer createServer(int port) {
        try {
            String portString = Integer.toString(port);
            return ServerRunner.createServerFromCommandLineArgs(
                    new String[] {"-inMemory", "-port", portString});
        } catch (Exception e) {
            logger.error("failed to create server runner", e);
            throw new IllegalArgumentException(e);
        }
    }

    private DynamoDbClient createClient(int port) {
        String endpoint = String.format("http://localhost:%d", port);
        logger.info("creating client for {}", endpoint);
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                // The region is meaningless for local DynamoDb but required for client builder validation
                .region(Region.US_WEST_2)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();
    }

    private int getFreePort() {
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

}
