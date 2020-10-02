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

package io.mantisrx.publish.proto;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InstanceInfo {

    private static final Logger LOG = LoggerFactory.getLogger(InstanceInfo.class);
    private final String applicationName;
    private final String zone;
    private final String clusterName;
    private final String autoScalingGroupName;
    private volatile InstanceStatus instanceStatus = InstanceStatus.UNKNOWN;

    public InstanceInfo(String applicationName, String zone, String clusterName, String autoScalingGroupName) {
        this.applicationName = applicationName;
        this.zone = zone;
        this.clusterName = clusterName;
        this.autoScalingGroupName = autoScalingGroupName;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getZone() {
        return zone;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getAutoScalingGroupName() {
        return autoScalingGroupName;
    }

    public InstanceStatus getInstanceStatus() {
        return this.instanceStatus;
    }

    public void setInstanceStatus(InstanceStatus status) {
        this.instanceStatus = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InstanceInfo that = (InstanceInfo) o;
        return Objects.equals(getApplicationName(), that.getApplicationName()) &&
            Objects.equals(getZone(), that.getZone()) &&
            Objects.equals(getClusterName(), that.getClusterName()) &&
            Objects.equals(getAutoScalingGroupName(), that.getAutoScalingGroupName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getApplicationName(), getZone(), getClusterName(), getAutoScalingGroupName());
    }

    @Override
    public String toString() {
        return "InstanceInfo{" +
            "applicationName='" + applicationName + '\'' +
            ", zone='" + zone + '\'' +
            ", clusterName='" + clusterName + '\'' +
            ", autoScalingGroupName='" + autoScalingGroupName + '\'' +
            ", instanceStatus=" + instanceStatus +
            '}';
    }

    public enum InstanceStatus {
        UP, // Ready to receive traffic
        DOWN, // Do not send traffic- healthcheck callback failed
        STARTING, // Just about starting- initializations to be done - do not
        // send traffic
        OUT_OF_SERVICE, // Intentionally shutdown for traffic
        UNKNOWN;

        public static InstanceStatus toEnum(String s) {
            if (s != null) {
                try {
                    return InstanceStatus.valueOf(s.toUpperCase());
                } catch (IllegalArgumentException e) {
                    // ignore and fall through to unknown
                    LOG.debug("illegal argument supplied to InstanceStatus.valueOf: {}, defaulting to {}", s, UNKNOWN);
                }
            }
            return UNKNOWN;
        }
    }
}
