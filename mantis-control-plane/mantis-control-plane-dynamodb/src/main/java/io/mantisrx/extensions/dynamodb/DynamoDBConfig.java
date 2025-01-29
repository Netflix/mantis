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

import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;

public interface DynamoDBConfig {
    @Config("mantis.ext.dynamodb.store.table")
    @DefaultNull
    String getDynamoDBStoreTable();

    @Config("mantis.ext.dynamodb.leader.table")
    @DefaultNull
    String getDynamoDBLeaderTable();

    @Config("mantis.ext.dynamodb.leader.key")
    @Default("mantis-leader")
    String getDynamoDBLeaderKey();

    @Config("mantis.ext.dynamodb.leader.leaseDuration")
    @Default("PT6S")
    String getDynamoDBLeaderLeaseDuration();

    @Config("mantis.ext.dynamodb.leader.heartbeatDuration")
    @Default("PT2S")
    String getDynamoDBLeaderHeartbeatDuration();

    @Config("mantis.ext.dynamodb.leader.heartbeatInBackground")
    @Default("true")
    boolean getDynamoDBLeaderHeartbeatInBackground();

    @Config("mantis.ext.dynamodb.monitor.gracefulShutdownDuration")
    @Default("PT2S")
    String getDynamoDBMonitorGracefulShutdownDuration();

    @Config("mantis.ext.dynamodb.region")
    @DefaultNull
    String getDynamoDBRegion();

    @Config("mantis.ext.dynamodb.endpointOverride")
    @DefaultNull
    String getDynamoDBEndpointOverride();

    @Config("mantis.ext.dynamodb.assumeRoleARN")
    @DefaultNull
    String getDynamoDBAssumeRoleARN();

    @Config("mantis.ext.dynamodb.assumeRoleSessionName")
    @DefaultNull
    String getDynamoDBAssumeRoleSessionName();

    @Config("mantis.ext.dynamodb.useLocal")
    @Default("false")
    boolean getDynamoDBUseLocal();

    @Config("mantis.ext.dynamodb.enableShutdownHook")
    @Default("true")
    boolean getEnableShutdownHook();
}
