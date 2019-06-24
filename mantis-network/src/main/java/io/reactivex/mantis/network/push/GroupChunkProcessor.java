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

package io.reactivex.mantis.network.push;

import java.util.List;
import java.util.Map;


public class GroupChunkProcessor<T> extends ChunkProcessor<T> {

    public GroupChunkProcessor(Router<T> router) {
        super(router);
    }

    @Override
    public void process(ConnectionManager<T> connectionManager, List<T> chunks) {
        Map<String, ConnectionGroup<T>> groups = connectionManager.groups();
        for (ConnectionGroup<T> group : groups.values()) {
            router.route(group.getConnections(), chunks);
        }
    }

}
