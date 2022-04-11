/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.server.master.resourcecluster;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

/**
 * This is a helper abstraction to find cluster ids for task executors.
 * For the foreseeable future, we expect only one implementation of this particular class.
 */
public interface ResourceClusterTaskExecutorMapper {
    @Nullable
    ClusterID getClusterFor(TaskExecutorID taskExecutorID);

    void onTaskExecutorDiscovered(ClusterID clusterID, TaskExecutorID taskExecutorID);

    static ResourceClusterTaskExecutorMapper inMemory() {
        return new ResourceClusterTaskExecutorMapper() {
            private final ConcurrentMap<TaskExecutorID, ClusterID> map =
                new ConcurrentHashMap<>();

            @Override
            public ClusterID getClusterFor(TaskExecutorID taskExecutorID) {
                return map.get(taskExecutorID);
            }

            @Override
            public void onTaskExecutorDiscovered(ClusterID clusterID, TaskExecutorID taskExecutorID) {
                map.putIfAbsent(taskExecutorID, clusterID);
            }
        };
    }
}
