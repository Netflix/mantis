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
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.swing.SingleSelectionModel;
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
    private static final Logger logger = LoggerFactory.getLogger(SingleSelectionModel.class);

    private static AmazonDynamoDBLockClient instanceLockClient;

    private static DynamoDbClient instanceClient;

    private static String partitionKey;

    private static Duration pollInterval;

    private static Duration gracefulShutdownDuration;

    private static DynamoDBConfig conf;
    private static Duration leaseDuration;
    private static String leaderTable;
    private static Boolean heartbeatInBackground;
    private static String kvStoreTable;

    private DynamoDBClientSingleton() {
        // Private constructor to prevent instantiation
    }

    public static synchronized AmazonDynamoDBLockClient getLockClient() {
        if (instanceLockClient == null) {
            instanceLockClient = new AmazonDynamoDBLockClient(
                AmazonDynamoDBLockClientOptions.builder(getDynamoDBClient(), getLeaderTable())
                    .withLeaseDuration(getLeaseDuration().toMillis())
                    .withHeartbeatPeriod(getPollInterval().toMillis())
                    .withCreateHeartbeatBackgroundThread(getHeartbeatInBackground())
                    .withTimeUnit(TimeUnit.MILLISECONDS)
                    .build());
        }
        return instanceLockClient;
    }

    public static synchronized String getKeyValueStoreTable() {
        if(kvStoreTable == null) {
           final String table = getDynamoDBConf().getDynamoDBStoreTable();
           if(table == null || table.isEmpty()) {
               throw new IllegalArgumentException("mantis.ext.dynamodb.store.table is null or empty and must be set to use DynamoDB as the key value store");
           }
           kvStoreTable = table;
        }
        return kvStoreTable;
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

    public static synchronized Duration getPollInterval() {
        if(pollInterval == null) {
            getDynamoDBConf();
        }
        return pollInterval;
    }

    public static synchronized Duration getGracefulShutdownDuration() {
        if(gracefulShutdownDuration == null) {
            getDynamoDBConf();
        }
        return gracefulShutdownDuration;
    }

    private static synchronized Boolean getHeartbeatInBackground() {
        if(heartbeatInBackground == null) {
            getDynamoDBConf();
        }
        return heartbeatInBackground;
    }

    private static synchronized  String getLeaderTable() {
        if(leaderTable == null) {
            final String tableName = getDynamoDBConf().getDynamoDBLeaderTable();
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("mantis.ext.dynamodb.leader.table is null or empty and must be set");
            }
            leaderTable = tableName;
        }
        return leaderTable;
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

    private static synchronized Duration getLeaseDuration() {
        if(leaseDuration == null) {
            getDynamoDBConf();
        }
        return leaseDuration;
    }

    private static synchronized DynamoDBConfig getDynamoDBConf() {
        if (conf == null) {
            final Properties props = DynamoDBConfig.getDynamoDBProperties();
            final ConfigurationObjectFactory configurationObjectFactory = new ConfigurationObjectFactory(props);
            conf =
                configurationObjectFactory.build(DynamoDBConfig.class);

            gracefulShutdownDuration = Duration.parse(conf.getDynamoDBMonitorGracefulShutdownDuration());
            pollInterval = Duration.parse(conf.getDynamoDBLeaderHeartbeatDuration());
            leaseDuration = Duration.parse(conf.getDynamoDBLeaderLeaseDuration());
            heartbeatInBackground = conf.getDynamoDBLeaderHeartbeatInBackground();
        }
        return conf;
    }

}
