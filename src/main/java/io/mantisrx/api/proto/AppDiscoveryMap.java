/**
 * Copyright 2018 Netflix, Inc.
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
package io.mantisrx.api.proto;

import io.mantisrx.server.core.JobSchedulingInfo;

import java.util.HashMap;
import java.util.Map;

public class AppDiscoveryMap {
    public final String version;
    public final Long timestamp;
    public final Map<String, Map<String, JobSchedulingInfo>> mappings = new HashMap<>();

    public AppDiscoveryMap(String version, Long timestamp) {
        this.version = version;
        this.timestamp = timestamp;
    }

    public void addMapping(String app, String stream, JobSchedulingInfo schedulingInfo) {
        if(!mappings.containsKey(app)) {
            mappings.put(app, new HashMap<String, JobSchedulingInfo>());
        }
        mappings.get(app).put(stream, schedulingInfo);
    }
}
