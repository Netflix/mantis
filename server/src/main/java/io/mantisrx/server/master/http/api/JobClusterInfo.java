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

package io.mantisrx.server.master.http.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.master.store.NamedJob;


public class JobClusterInfo {

    private final String name;
    private final String latestVersion;
    private final NamedJob.SLA sla;
    private final JobOwner owner;
    private final boolean disabled;
    private final boolean cronActive;
    private final List<JarInfo> jars;
    private final List<Parameter> parameters;
    private final List<Label> labels;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobClusterInfo(
            @JsonProperty("name") String name,
            @JsonProperty("sla") NamedJob.SLA sla,
            @JsonProperty("owner") JobOwner owner,
            @JsonProperty("disabled") boolean disabled,
            @JsonProperty("cronActive") boolean cronActive,
            @JsonProperty("jars") List<JarInfo> jars,
            @JsonProperty("parameters") List<Parameter> parameters,
            @JsonProperty("labels") List<Label> labels
    ) {
        this.name = name;
        this.sla = sla;
        this.owner = owner;
        this.disabled = disabled;
        this.cronActive = cronActive;
        this.jars = jars;
        this.labels = labels;
        if (jars == null || jars.isEmpty())
            latestVersion = "";
        else {
            JarInfo latest = null;
            for (JarInfo ji : jars) {
                if (latest == null || ji.uploadedAt > latest.uploadedAt) {
                    latest = ji;
                }
            }
            latestVersion = latest == null ? "" : latest.version;
        }
        this.parameters = parameters;
    }

    public String getName() {
        return name;
    }

    public String getLatestVersion() {
        return latestVersion;
    }

    public NamedJob.SLA getSla() {
        return sla;
    }

    public JobOwner getOwner() {
        return owner;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean isCronActive() {
        return cronActive;
    }

    public List<JarInfo> getJars() {
        return jars;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public List<Label> getLabels() {
        return this.labels;
    }

    public static class JarInfo {

        private final String version;
        private final long uploadedAt;
        private final String url;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public JarInfo(@JsonProperty("version") String version, @JsonProperty("uploadedAt") long uploadedAt,
                       @JsonProperty("url") String url) {
            this.version = version;
            this.uploadedAt = uploadedAt;
            this.url = url;
        }

        public String getVersion() {
            return version;
        }

        public long getUploadedAt() {
            return uploadedAt;
        }

        public String getUrl() {
            return url;
        }
    }
}
