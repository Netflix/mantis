package io.mantisrx.extensions.dynamodb;

import io.mantisrx.server.core.utils.ConfigUtils;
import java.lang.reflect.Method;
import java.util.Properties;
import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;

public interface DynamoDBConfig {
    String DYNAMO_DB_PROPERTIES_KEY = "mantis.ext.dynamodb.properties.file";

    /**
     * This is a helper function to load configuration using the same library as Mantis. There
     * are other solutions that can do this, but the preference is to use current libraries.
     * The function enables users to configure the name of the property file and will use the
     * following precedence order:
     *  1. Properties file
     *  2. Environment Variables matching convention from Mantis ConfigUtils.loadProperties
     *  3. Java system variables
     *
     * @return {@link Properties} merging in the precedence order file, env, java system.
     */
    static Properties getDynamoDBProperties() {
        final String propFile = System.getProperty(DYNAMO_DB_PROPERTIES_KEY, "dynamodb.properties");
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
                // Override any value that is in system properties
                for (String key: config.value()) {
                    final String value = System.getProperty(key);
                    if (value != null && !value.isEmpty()) {
                        dynamodbProps.setProperty(key, System.getProperty(key));
                    }
                }
            }
        }
        return dynamodbProps;
    }

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

    // Currently primarily used for test scenarios
    @Config("mantis.ext.dynamodb.useStaticCredentials")
    @Default("false")
    boolean getDynamoDBUseStaticCredentials();

    @Config("mantis.ext.dynamodb.assumeRoleARN")
    @DefaultNull
    String getDynamoDBAssumeRoleARN();

    @Config("mantis.ext.dynamodb.assumeRoleSessionName")
    @DefaultNull
    String getDynamoDBAssumeRoleSessionName();

    @Config("mantis.ext.dynamodb.useLocal")
    @Default("false")
    boolean getDynamoDBUseLocal();
}
