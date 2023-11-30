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

import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;


/**
 * An abstraction for storage api that behaves like a key-value storage
 * like apache-cassandra.
 * See {@link FileBasedStore}
 * for implementation using files.
 * TODO(hmittal): Add an implementation using SQL, apache-cassandra
 */
public interface KeyValueStore {

    KeyValueStore NO_OP = new NoopStore();

    static KeyValueStore noop() {
        return NO_OP;
    }

    static KeyValueStore inMemory() {
        return new InMemoryStore();
    }

    /**
     * Gets all rows from the table
     *
     * @param tableName the tableName/table to read from
     * @return map partition key to map of secondary keys to actual data
     */
    default Map<String, Map<String, String>> getAllRows(String tableName) throws IOException {
        Map<String, Map<String, String>> results = new HashMap<>();
        for (String pKey : getAllPartitionKeys(tableName)) {
            results.computeIfAbsent(pKey, (k) -> new HashMap<>());
            results.get(pKey).putAll(getAll(tableName, pKey));
        }
        return results;
    }

    /**
     * Gets all partition keys from the table.
     * This could be beneficial to call instead of getAllRows
     * if the data volume in the table is large and you want
     * to process rows iteratively.
     *
     * It iterates on partitionKey instead of primaryKey to
     * prevent keys from the same partition coming out of order.
     *
     * @param tableName the table to read from
     * @return list of all partition keys
     */
    List<String> getAllPartitionKeys(String tableName) throws IOException;

    /**
     * Gets the row corresponding to primary key (partitionKey, secondaryKey)
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param secondaryKey secondaryKey for the record
     * @return data
     */
    default String get(String tableName, String partitionKey, String secondaryKey) throws IOException {
        return getAll(tableName, partitionKey).get(secondaryKey);
    }

    /**
     * Gets all rows corresponding to partition key
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @return all records corresponding to partitionKey as a map of secondaryKey -> data
     */
    Map<String, String> getAll(String tableName, String partitionKey) throws IOException;

    /**
     * Adds a row corresponding to primary key (partitionKey, secondaryKey)
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param secondaryKey secondaryKey for the record
     * @param data the actual data
     * @return boolean if the data was saved
     */
    default boolean upsert(String tableName, String partitionKey, String secondaryKey, String data) throws IOException {
        return upsertAll(tableName, partitionKey, ImmutableMap.of(secondaryKey, data));
    }

    /**
     * Adds a row corresponding to primary key (partitionKey, secondaryKey)
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param secondaryKey secondaryKey for the record
     * @param data the actual data
     * @param ttl ttl for the record in millis
     * @return boolean if the data was saved
     */
    default boolean upsert(String tableName, String partitionKey, String secondaryKey, String data, Duration ttl) throws IOException {
        return upsertAll(tableName, partitionKey, ImmutableMap.of(secondaryKey, data), ttl);
    }

    /**
     * Adds all row corresponding to partition key.
     * The rows are passed as a map of secondaryKey -> data
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param all map of rows
     * @return boolean if the data was saved
     */
    default boolean upsertAll(String tableName, String partitionKey, Map<String, String> all) throws IOException {
        return upsertAll(tableName, partitionKey, all, Duration.ZERO);
    }

    /**
     * Adds all row corresponding to partition key.
     * The rows are passed as a map of secondaryKey -> data
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param all map of rows
     * @param ttl ttl for the record in millis (use null or Duration.ZERO for no expiry)
     * @return boolean if the data was saved
     */
    boolean upsertAll(String tableName, String partitionKey, Map<String, String> all, Duration ttl) throws IOException;

    default boolean upsertOrdered(String tableName, String partitionKey, String secondaryKey, Instant orderingTimestamp, String value, Duration ttl) throws IOException {
        return upsertOrdered(tableName, partitionKey, ImmutableMap.of(Tuple.of(secondaryKey, orderingTimestamp), value), ttl);
    }

    /**
     * Adds all rows corresponding to partition key in an ordered manner determined by the secondary key.
     * @param tableName
     * @param partitionKey
     * @param all
     * @param ttl
     * @return
     * @throws IOException
     */
    default boolean upsertOrdered(String tableName, String partitionKey, Map<Tuple2<String, Instant>, String> all, Duration ttl) throws IOException {
        Map<String, String> items =
            all.entrySet().stream()
                .map(entry -> {
                    String key = entry.getKey()._2.toEpochMilli() + "," + entry.getKey()._1;
                    return Tuple.of(
                        key,
                        entry.getValue());
                }).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        return upsertAll(tableName, partitionKey, items, ttl);
    }

    default Map<String, String> getAllOrdered(String tableName, String partitionKey, Instant from, Instant to) throws IOException {
        Map<String, String> items = getAll(tableName, partitionKey);
        if (items == null || items.isEmpty()) {
            return Collections.emptyMap();
        }

        Pattern pattern = Pattern.compile("^(\\d+),(.*)$");

        Map<String, String> results = new LinkedHashMap<>();
        items.entrySet().stream()
            .filter(entry -> {
                Matcher matcher = pattern.matcher(entry.getKey());
                if (!matcher.matches()) {
                    return false;
                }

                long ts = Long.parseLong(matcher.group(1));
                return ts >= from.toEpochMilli() && ts < to.toEpochMilli();
            })
            .forEach(entry -> results.put(entry.getKey(), entry.getValue()));
        return results;
    }

    /**
     * Deletes a row corresponding to the primary key (partitionKey, secondaryKey)
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param secondaryKey secondaryKey for the record
     * @return boolean if row was deleted
     */
    boolean delete(String tableName, String partitionKey, String secondaryKey) throws IOException;

    default boolean deleteOrdered(String tableName, String partitionKey, String secondaryKey, Instant orderingTimestamp) throws IOException {
        return delete(tableName, partitionKey, orderingTimestamp.toEpochMilli() + "," + secondaryKey);
    }

    /**
     * Deletes all rows corresponding to a partition key
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @return boolean if the rows were deleted
     */
    boolean deleteAll(String tableName, String partitionKey) throws IOException;

    /**
     * Helpful method to determine if a row exists in the table
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param secondaryKey secondaryKey for the record
     * @return boolean if row exists
     */
    default boolean isRowExists(String tableName, String partitionKey, String secondaryKey) throws IOException {
        Map<String, String> items = getAll(tableName, partitionKey);
        return items != null && items.containsKey(secondaryKey);
    }

    /**
     * Allows searching for all rows that share the prefix (in secondary keys) for partitionKey
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param prefix secondaryKey for the record; null or blank values are default-ed to empty string
     * @return
     */
    default Map<String, String> getAllWithPrefix(String tableName, String partitionKey, String prefix) throws IOException {
        String pr = StringUtils.defaultIfBlank(prefix, "");
        return getAll(tableName, partitionKey).entrySet()
            .stream().filter(x -> StringUtils.startsWith(x.getKey(), pr))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    }

    class NoopStore implements KeyValueStore {

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

    class InMemoryStore implements KeyValueStore {

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
            SortedMap<String, String> items = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
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
