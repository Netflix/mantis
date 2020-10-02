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

import java.util.Optional;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;


public class JobId implements Comparable<JobId> {

    private static final String DELIMITER = "-";
    private final String cluster;
    private final long jobNum;
    private final String id;

    /**
     * @param clusterName The Job Cluster that this jobID belongs to
     * @param jobNum      Identifies the job for this cluster
     */
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobId(@JsonProperty("clusterName") final String clusterName,
                 @JsonProperty("jobNum") final long jobNum) {
        this.cluster = clusterName;
        this.jobNum = jobNum;
        this.id = clusterName + DELIMITER + jobNum;
    }

    /*
    Returns a valid JobId if the passed 'id' string is wellformed
     */
    public static Optional<JobId> fromId(final String id) {
        final int i = id.lastIndexOf(DELIMITER);
        if (i < 0) {
            return Optional.empty();
        }
        final String jobCluster = id.substring(0, i);
        try {
            final int jobNum = Integer.parseInt(id.substring(i + 1));
            return Optional.ofNullable(new JobId(jobCluster, jobNum));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }

    public String getCluster() {
        return cluster;
    }

    public long getJobNum() {
        return jobNum;
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + (int) (jobNum ^ (jobNum >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JobId other = (JobId) obj;
        if (cluster == null) {
            if (other.cluster != null)
                return false;
        } else if (!cluster.equals(other.cluster))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return jobNum == other.jobNum;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public int compareTo(JobId o) {

        return id.compareTo(o.getId());
    }
}
