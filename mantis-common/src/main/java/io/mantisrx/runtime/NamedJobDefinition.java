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

package io.mantisrx.runtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


public class NamedJobDefinition {

    private final MantisJobDefinition jobDefinition;

    ;
    private final JobOwner owner;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public NamedJobDefinition(@JsonProperty("jobDefinition") MantisJobDefinition jobDefinition,
                              @JsonProperty("owner") JobOwner owner) {
        this.jobDefinition = jobDefinition;
        this.owner = owner;
    }

    public MantisJobDefinition getJobDefinition() {
        return jobDefinition;
    }

    public JobOwner getOwner() {
        return owner;
    }

    @Override
    public String toString() {
        return "NamedJobDefinition{" +
                "jobDefinition=" + jobDefinition +
                ", owner=" + owner +
                '}';
    }

    public enum CronPolicy {KEEP_EXISTING, KEEP_NEW}
}
