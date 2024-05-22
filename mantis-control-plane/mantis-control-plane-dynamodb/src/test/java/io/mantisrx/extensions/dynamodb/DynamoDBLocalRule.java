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
