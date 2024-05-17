package io.mantisrx.server.master.store;

import io.mantisrx.server.core.KeyValueStore;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryStore implements KeyValueStore {

    public static KeyValueStore inMemory() {
        return new InMemoryStore();
    }

    // table -> partitionKey -> secondaryKey -> data
    private final Map<String, Map<String, SortedMap<String, String>>> store = new ConcurrentHashMap<>();

    @Override
    public List<String> getAllPartitionKeys(String tableName) {
        if (store.get(tableName) == null) {
            return Collections.emptyList();
        } else{
            return new ArrayList<>(store.get(tableName).keySet());
        }
    }

    @Override
    public Map<String, String> getAll(String tableName, String partitionKey)
            throws IOException {
        if (store.get(tableName) == null) {
            return Collections.emptyMap();
        } else if (store.get(tableName).get(partitionKey) == null) {
            return Collections.emptyMap();
        } else {
            return store.get(tableName).get(partitionKey);
        }
    }

    @Override
    public boolean upsertAll(String tableName, String partitionKey, Map<String, String> all,
                             Duration ttl) throws IOException {
        store.putIfAbsent(tableName, new ConcurrentHashMap<>());
        SortedMap<String, String> items =
                store.get(tableName)
                        .getOrDefault(partitionKey, new ConcurrentSkipListMap<>(Comparator.reverseOrder()));
        items.putAll(all);
        store.get(tableName).put(partitionKey, items);
        return true;
    }

    @Override
    public boolean delete(String tableName, String partitionKey, String secondaryKey)
            throws IOException {
        if (store.containsKey(tableName) && // table exists
                store.get(tableName).containsKey(partitionKey) && // partitionKey exists
                store.get(tableName).get(partitionKey).containsKey(secondaryKey)) { // secondaryKey exists
            store.get(tableName).get(partitionKey).remove(secondaryKey);
            return true;
        }
        return false;
    }

    @Override
    public boolean deleteAll(String tableName, String partitionKey) throws IOException {
        if (store.containsKey(tableName) && // table exists
                store.get(tableName).containsKey(partitionKey)) { // partitionKey exists
            store.get(tableName).remove(partitionKey);
            return true;
        }
        return false;
    }
}
