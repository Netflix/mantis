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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface MantisStorageProvider {

    /**
     * Gets all rows from the table
     *
     * @param tableName the tableName/table to read from
     * @return map partition key to map of secondary keys to actual data
     */
    Map<String, Map<String, String>> getAllRows(String tableName) throws IOException;

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
    String get(String tableName, String partitionKey, String secondaryKey) throws IOException;

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
    boolean upsert(String tableName, String partitionKey, String secondaryKey, String data) throws IOException;

    /**
     * Adds all row corresponding to partition key.
     * The rows are passed as a map of secondaryKey -> data
     * @param tableName the tableName/table to read from
     * @param partitionKey partitionKey for the record
     * @param all map of rows
     * @return boolean if the data was saved
     */
    boolean upsertAll(String tableName, String partitionKey, Map<String, String> all) throws IOException;

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
}
