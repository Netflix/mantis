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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import io.mantisrx.server.core.utils.ConfigUtils;
import java.lang.reflect.Method;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.swing.SingleSelectionModel;
import org.skife.config.Config;
import org.skife.config.ConfigurationObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class DynamoDBClientSingleton {
    public static final String DYNAMO_DB_PROPERTIES_KEY = "mantis.ext.dynamodb.properties.file";

    private static final Logger logger = LoggerFactory.getLogger(SingleSelectionModel.class);
    private static AmazonDynamoDBLockClient instanceLockClient;
    private static DynamoDbClient instanceClient;
    private static String partitionKey;
    private static DynamoDBConfig conf;

    private DynamoDBClientSingleton() {
        // Private constructor to prevent instantiation
    }

    public static synchronized AmazonDynamoDBLockClient getLockClient() {
        if (instanceLockClient == null) {
            final DynamoDBConfig conf = getDynamoDBConf();
            instanceLockClient = new AmazonDynamoDBLockClient(
                AmazonDynamoDBLockClientOptions.builder(getDynamoDBClient(), conf.getDynamoDBLeaderTable())
                    .withLeaseDuration(Duration.parse(conf.getDynamoDBLeaderLeaseDuration()).toMillis())
                    .withHeartbeatPeriod(Duration.parse(conf.getDynamoDBLeaderHeartbeatDuration()).toMillis())
                    .withCreateHeartbeatBackgroundThread(conf.getDynamoDBLeaderHeartbeatInBackground())
                    .withTimeUnit(TimeUnit.MILLISECONDS)
                    .build());
        }
        return instanceLockClient;
    }

    public static synchronized String getPartitionKey() {
        if(partitionKey == null) {
            partitionKey = getDynamoDBConf().getDynamoDBLeaderKey();

            if (partitionKey == null || partitionKey.isEmpty()) {
                throw new IllegalArgumentException("mantis.ext.dynamodb.leader.key is null or empty and must be set");
            }
        }
        return partitionKey;
    }

    public static synchronized DynamoDbClient getDynamoDBClient() {
        if (instanceClient == null) {
            final DynamoDBConfig conf = getDynamoDBConf();
            final DynamoDbClientBuilder builder = DynamoDbClient.builder();
            if (conf.getDynamoDBRegion() != null && !conf.getDynamoDBRegion().isEmpty()) {
                builder.region(Region.of(conf.getDynamoDBRegion()));
            }
            if (conf.getDynamoDBEndpointOverride() != null && !conf.getDynamoDBEndpointOverride().isEmpty()) {
                final URI uri = URI.create(conf.getDynamoDBEndpointOverride());
                builder.endpointOverride(uri);
                logger.warn("using endpoint override of {}", uri);
            }
            useAssumeRole(conf).ifPresent((c) -> builder.credentialsProvider(StaticCredentialsProvider.create(c)));
            if (conf.getDynamoDBUseLocal()) {
                final AwsBasicCredentials credentials = AwsBasicCredentials
                    .create("fakeAccessKeyId", "fakeSecretAccessKey");
                builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
                logger.warn("using local dynamodb this should not be used in production");
            }
            instanceClient = builder.build();
        }
        return instanceClient;
    }

    public static synchronized DynamoDBConfig getDynamoDBConf() {
        if (conf == null) {
            final String propFile = System.getProperty(DYNAMO_DB_PROPERTIES_KEY, "dynamodb.properties");
            // This checks for conventional env variable overrides
            final Properties dynamodbProps = ConfigUtils.loadProperties(propFile);
            // Make sure the provided class is an interface
            if (!DynamoDBConfig.class.isInterface()) {
                throw new IllegalArgumentException("The class must be an interface.");
            }
            // Iterate over the methods of the interface
            for (Method method : DynamoDBConfig.class.getDeclaredMethods()) {
                // Check if the method is annotated with @Config
                if (method.isAnnotationPresent(Config.class)) {
                    Config config = method.getAnnotation(Config.class);
                    // Override any value that is in java system properties
                    for (String key: config.value()) {
                        final String value = System.getProperty(key);
                        if (value != null && !value.isEmpty()) {
                            dynamodbProps.setProperty(key, System.getProperty(key));
                        }
                    }
                }
            }
            conf = new ConfigurationObjectFactory(dynamodbProps).build(DynamoDBConfig.class);
        }
        return conf;
    }

    private static Optional<AwsSessionCredentials> useAssumeRole(DynamoDBConfig config) {
        final String roleARN = config.getDynamoDBAssumeRoleARN();
        final String roleSessionName = config.getDynamoDBAssumeRoleSessionName();
        if (roleARN == null && roleSessionName == null) {
            return Optional.empty();
        }
        if (roleARN == null || roleARN.isEmpty() || roleSessionName == null || roleSessionName.isEmpty()) {
            throw new IllegalArgumentException(String.format("received invalid assume role configuration ARN [%s] Session Name [%s]", roleARN, roleSessionName));
        }
        StsClient stsClient = StsClient.builder().region(Region.AWS_GLOBAL).build();
        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
            .roleArn(roleARN)
            .roleSessionName(roleSessionName)
            .build();
        AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);

        Credentials temporaryCredentials = assumeRoleResponse.credentials();

        return Optional.of(
            AwsSessionCredentials.create(
                temporaryCredentials.accessKeyId(),
                temporaryCredentials.secretAccessKey(),
                temporaryCredentials.sessionToken()));
    }
}
