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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobSla {

    public static final String uniqueTagName = "unique";
    private static final Logger logger = LoggerFactory.getLogger(JobSla.class);
    private final long runtimeLimitSecs;
    private final long minRuntimeSecs;
    private final StreamSLAType slaType;
    private final MantisJobDurationType durationType;
    private final String userProvidedType;
    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobSla(@JsonProperty("runtimeLimitSecs") long runtimeLimitSecs,
                  @JsonProperty("minRuntimeSecs") long minRuntimeSecs,
                  @JsonProperty("slaType") StreamSLAType slaType,
                  @JsonProperty("durationType") MantisJobDurationType durationType,
                  @JsonProperty("userProvidedType") String userProvidedType) {
        this.runtimeLimitSecs = Math.max(0L, runtimeLimitSecs);
        this.minRuntimeSecs = Math.max(0L, minRuntimeSecs);
        this.slaType = slaType == null ? StreamSLAType.Lossy : slaType;
        this.durationType = durationType;
        this.userProvidedType = userProvidedType;
    }

    public long getRuntimeLimitSecs() {
        return runtimeLimitSecs;
    }

    public long getMinRuntimeSecs() {
        return minRuntimeSecs;
    }

    public StreamSLAType getSlaType() {
        return slaType;
    }

    public MantisJobDurationType getDurationType() {
        return durationType;
    }

    public String getUserProvidedType() {
        return userProvidedType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((durationType == null) ? 0 : durationType.hashCode());
        result = prime * result + (int) (minRuntimeSecs ^ (minRuntimeSecs >>> 32));
        result = prime * result + (int) (runtimeLimitSecs ^ (runtimeLimitSecs >>> 32));
        result = prime * result + ((slaType == null) ? 0 : slaType.hashCode());
        result = prime * result + ((userProvidedType == null) ? 0 : userProvidedType.hashCode());
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
        JobSla other = (JobSla) obj;
        if (durationType != other.durationType)
            return false;
        if (minRuntimeSecs != other.minRuntimeSecs)
            return false;
        if (runtimeLimitSecs != other.runtimeLimitSecs)
            return false;
        if (slaType != other.slaType)
            return false;
        if (userProvidedType == null) {
            if (other.userProvidedType != null)
                return false;
        } else if (!userProvidedType.equals(other.userProvidedType))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "JobSla [runtimeLimitSecs=" + runtimeLimitSecs + ", minRuntimeSecs=" + minRuntimeSecs + ", slaType="
                + slaType + ", durationType=" + durationType + ", userProvidedType=" + userProvidedType + "]";
    }

    public enum StreamSLAType {
        Lossy
    }

    public static class Builder {

        private static final ObjectMapper objectMapper = new ObjectMapper();
        private long runtimeLimit = 0L;
        private long minRuntimeSecs = 0L;
        private StreamSLAType slaType = StreamSLAType.Lossy;
        private MantisJobDurationType durationType = MantisJobDurationType.Perpetual;
        private Map<String, String> userProvidedTypes = new HashMap<>();

        public Builder withRuntimeLimit(long limit) {
            this.runtimeLimit = limit;
            return this;
        }

        public Builder withMinRuntimeSecs(long minRuntimeSecs) {
            this.minRuntimeSecs = minRuntimeSecs;
            return this;
        }

        public Builder withSlaType(StreamSLAType slaType) {
            this.slaType = slaType;
            return this;
        }

        public Builder withDurationType(MantisJobDurationType durationType) {
            this.durationType = durationType;
            return this;
        }

        // Sets the job's unique tag value which is used to determine if two jobs are identical.
        // This is primarily used to determine if a job already exists and connect to it instead of submitting a
        // duplicate identical job.
        public Builder withUniqueJobTagValue(String value) {
            userProvidedTypes.put(uniqueTagName, value);
            return this;
        }

        public Builder withUserTag(String key, String value) {
            userProvidedTypes.put(key, value);
            return this;
        }

        public JobSla build() {
            try {
                return new JobSla(runtimeLimit, minRuntimeSecs, slaType, durationType, objectMapper.writeValueAsString(userProvidedTypes));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unexpected error creating json out of user tags map: " + e.getMessage(), e);
            }
        }
    }
}
