package io.mantisrx.extensions.dynamodb;

import org.junit.rules.ExternalResource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

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

    public DynamoDbClient getDynamoDbClient() {
        return server.getDynamoDbClient();
    }
}
