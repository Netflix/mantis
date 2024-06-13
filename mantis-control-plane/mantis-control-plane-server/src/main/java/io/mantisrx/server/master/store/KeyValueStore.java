/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.server.master.store;

import io.mantisrx.server.core.IKeyValueStore;
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


/**
 * The interface has moved to see:
 * {@link IKeyValueStore}
 * The move was to enable extensions to provide this implementation
 * with a clean dependency graph.
 * TODO(hmittal): Add an implementation using SQL, apache-cassandra
 */
@Deprecated
public interface KeyValueStore extends IKeyValueStore {

    IKeyValueStore NO_OP = new NoopStore();

    static IKeyValueStore noop() {
        return NO_OP;
    }

    @Deprecated
    static IKeyValueStore inMemory() { return new io.mantisrx.server.master.store.InMemoryStore();
    }

    /**
     * See {@link io.mantisrx.server.master.store.NoopStore}
     */
    @Deprecated
    class NoopStore implements IKeyValueStore {

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

    /**
     * See {@link io.mantisrx.server.master.store.InMemoryStore}
     */
    @Deprecated
    class InMemoryStore implements IKeyValueStore {

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
}
