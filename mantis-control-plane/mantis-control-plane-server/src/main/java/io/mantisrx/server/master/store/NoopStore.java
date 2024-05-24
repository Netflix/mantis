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
package io.mantisrx.server.master.store;

import io.mantisrx.server.core.IKeyValueStore;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class NoopStore implements IKeyValueStore {

    static IKeyValueStore NO_OP = new NoopStore();

    public static IKeyValueStore noop() {
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
