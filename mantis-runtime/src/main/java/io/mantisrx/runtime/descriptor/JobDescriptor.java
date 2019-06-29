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

package io.mantisrx.runtime.descriptor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


public class JobDescriptor {

    private JobInfo jobInfo;
    private String project;
    private String version;
    private long timestamp;
    // flag for rolling out the job master changes, Job Master is luanched by MantisMaster only if this flag is set
    private boolean readyForJobMaster;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobDescriptor(
            @JsonProperty("jobInfo") JobInfo jobInfo,
            @JsonProperty("project") String project,
            @JsonProperty("version") String version,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("readyForJobMaster") boolean readyForJobMaster) {
        this.jobInfo = jobInfo;
        this.version = version;
        this.timestamp = timestamp;
        this.project = project;
        this.readyForJobMaster = readyForJobMaster;
    }

    public String getVersion() {
        return version;
    }

    public JobInfo getJobInfo() {
        return jobInfo;
    }

    public String getProject() {
        return project;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isReadyForJobMaster() {
        return readyForJobMaster;
    }
}
