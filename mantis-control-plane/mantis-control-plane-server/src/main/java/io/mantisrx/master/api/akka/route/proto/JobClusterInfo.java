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

package io.mantisrx.master.api.akka.route.proto;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;

public class JobClusterInfo {

    private final String name;
    private final String jobId;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    public JobClusterInfo(@JsonProperty("name") String name,
                        @JsonProperty("jobId") String jobId) {
        this.name = name;
        this.jobId = jobId;
    }

    public String getName() {
        return name;
    }

    public String getJobId() {
        return jobId;
    }

}