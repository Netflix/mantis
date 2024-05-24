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
package io.mantisrx.server.core;

import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.vavr.Tuple2;
import java.io.IOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;


/**
 * An abstraction for storage api that behaves like a key-value storage.
 *
 * Any storage system that implements this interface can be used to provide
 * high availability operations for the Mantis Master Server. Inside the
 * mantis-control-plane-server project are example implementations for file,
 * in-memory and no-op implementations.
 */
public interface IKeyValueStore {

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

        default boolean upsertOrdered(String tableName, String partitionKey, Long orderingId, String value, Duration ttl) throws IOException {
            return upsertOrdered(tableName, partitionKey, ImmutableMap.of(orderingId, value), ttl);
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
        default boolean upsertOrdered(String tableName, String partitionKey, Map<Long, String> all, Duration ttl) throws IOException {
            Map<String, String> items = all.entrySet().stream()
                    .map(e -> new Tuple2<>(Long.toString(e.getKey()), e.getValue()))
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
            return upsertAll(tableName, partitionKey, items, ttl);
        }

        default Map<Long, String> getAllOrdered(String tableName, String partitionKey, int limit) throws IOException {
            Map<String, String> items = getAll(tableName, partitionKey);
            Comparator<Tuple2<Long, String>> longOrder = Comparator.comparing(Tuple2::_1);
            Comparator<Tuple2<Long, String>> reverseOrder = longOrder.reversed();
            return items.entrySet().stream()
                    .map(e -> new Tuple2<>(Long.parseLong(e.getKey()), e.getValue()))
                    // reversed order
                    .sorted(reverseOrder)
                    .limit(limit)
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        }

        default Map<Long, String> getAllOrdered(String tableName, String partitionKey, int limit, long endExclusive) throws IOException {
            Map<String, String> items = getAll(tableName, partitionKey);
            Comparator<Tuple2<Long, String>> longOrder = Comparator.comparing(Tuple2::_1);
            Comparator<Tuple2<Long, String>> reverseOrder = longOrder.reversed();
            return items.entrySet().stream()
                    .map(e -> new Tuple2<>(Long.parseLong(e.getKey()), e.getValue()))
                    // reversed order
                    .sorted(reverseOrder)
                    .filter(e -> e._1 < endExclusive)
                    .limit(limit)
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        }

        /**
         * Deletes a row corresponding to the primary key (partitionKey, secondaryKey)
         * @param tableName the tableName/table to read from
         * @param partitionKey partitionKey for the record
         * @param secondaryKey secondaryKey for the record
         * @return boolean if row was deleted
         */
        boolean delete(String tableName, String partitionKey, String secondaryKey) throws IOException;

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
}
