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


package io.mantisrx.server.master.domain;

import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.google.common.base.Preconditions;
import java.util.Objects;


public class JobClusterConfig {

    private final String jobJarUrl;
    private final String artifactName;
    private final String version;
    private final long uploadedAt;
    private final SchedulingInfo schedulingInfo;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobClusterConfig(@JsonProperty("jobJarUrl") String jobJarUrl,
                            @JsonProperty("artifactName") String artifactName,
                            @JsonProperty("uploadedAt") long uploadedAt,
                            @JsonProperty("version") String version,
                            @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo

    ) {
        this.jobJarUrl = jobJarUrl;
        this.artifactName = artifactName;
        this.uploadedAt = uploadedAt;
        this.version = (version == null || version.isEmpty()) ?
                "" + System.currentTimeMillis() :
                version;
        this.schedulingInfo = schedulingInfo;

    }

    public String getJobJarUrl() {
        return jobJarUrl;
    }

    public String getArtifactName() {
        return artifactName;
    }

    public long getUploadedAt() {
        return uploadedAt;
    }

    public String getVersion() {
        return version;
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }


    @Override
    public String toString() {
        return "JobClusterConfig [jobJarUrl=" + jobJarUrl + ", artifactName=" + artifactName + ", version=" + version + ", uploadedAt=" + uploadedAt
                + ", schedulingInfo=" + schedulingInfo + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobClusterConfig that = (JobClusterConfig) o;
        return uploadedAt == that.uploadedAt &&
                Objects.equals(jobJarUrl, that.jobJarUrl) &&
                Objects.equals(artifactName, that.artifactName) &&
                Objects.equals(version, that.version) &&
                Objects.equals(schedulingInfo, that.schedulingInfo);
    }

    @Override
    public int hashCode() {

        return Objects.hash(jobJarUrl, artifactName, version, uploadedAt, schedulingInfo);
    }

    public static class Builder {

        String jobJarUrl;
        String artifactName;
        String version;
        long uploadedAt = -1;
        SchedulingInfo schedulingInfo;


        public Builder() {}

        public Builder withJobJarUrl(String jobJarUrl) {
            Preconditions.checkNotNull(jobJarUrl, "jobJarUrl cannot be null");
            Preconditions.checkArgument(!jobJarUrl.isEmpty(), "jobJarUrl cannot be empty");
            this.jobJarUrl = jobJarUrl;
            return this;
        }

        public Builder withArtifactName(String artifactName) {
            Preconditions.checkNotNull(artifactName, "artifactName cannot be null");
            Preconditions.checkArgument(!artifactName.isEmpty(), "ArtifactName cannot be empty");
            this.artifactName = artifactName;
            return this;
        }

        public Builder withVersion(String version) {
            Preconditions.checkNotNull(version, "version cannot be null");
            Preconditions.checkArgument(!version.isEmpty(), "version cannot be empty");
            this.version = version;
            return this;
        }

        public Builder withUploadedAt(long uAt) {
            Preconditions.checkArgument(uAt > 0, "uploaded At cannot be <= 0");
            this.uploadedAt = uAt;
            return this;
        }

        public Builder withSchedulingInfo(SchedulingInfo sInfo) {
            Preconditions.checkNotNull(sInfo, "schedulingInfo cannot be null");
            this.schedulingInfo = sInfo;
            return this;
        }

        public Builder from(JobClusterConfig config) {
            jobJarUrl = config.getJobJarUrl();
            artifactName = config.getArtifactName();
            version = config.getVersion();
            uploadedAt = config.getUploadedAt();
            schedulingInfo = config.getSchedulingInfo();
            return this;
        }

        // TODO add validity checks for SchedulingInfo, MachineDescription etc
        public JobClusterConfig build() {
            Preconditions.checkNotNull(jobJarUrl);
            Preconditions.checkNotNull(artifactName);
            Preconditions.checkNotNull(schedulingInfo);
            this.uploadedAt = (uploadedAt == -1) ? System.currentTimeMillis() : uploadedAt;
            this.version = (version == null || version.isEmpty()) ? "" + System.currentTimeMillis() : version;
            return new JobClusterConfig(jobJarUrl, artifactName, uploadedAt, version, schedulingInfo);
        }
    }


}
