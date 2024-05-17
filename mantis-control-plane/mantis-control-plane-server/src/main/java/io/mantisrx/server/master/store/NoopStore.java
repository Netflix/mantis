package io.mantisrx.server.master.store;

import io.mantisrx.server.core.KeyValueStore;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class NoopStore implements KeyValueStore {

    static KeyValueStore NO_OP = new NoopStore();

    public static KeyValueStore noop() {
        return NO_OP;
    }

    @Override
    public Map<String, Map<String, String>> getAllRows(String tableName) {
        return null;
    }

    @Override
    public List<String> getAllPartitionKeys(String tableName) {
        return null;
    }

    @Override
    public String get(String tableName, String partitionKey, String secondaryKey) {
        return null;
    }

    @Override
    public Map<String, String> getAll(String tableName, String partitionKey) {
        return null;
    }

    @Override
    public boolean upsertAll(String tableName, String partitionKey, Map<String, String> all, Duration ttl) {
        return false;
    }

    @Override
    public boolean delete(String tableName, String partitionKey, String secondaryKey) {
        return false;
    }

    @Override
    public boolean deleteAll(String tableName, String partitionKey) {
        return false;
    }
}
