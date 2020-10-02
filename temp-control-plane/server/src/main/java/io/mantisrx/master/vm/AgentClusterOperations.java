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

package io.mantisrx.master.vm;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.VirtualMachineCurrentState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AgentClusterOperations {

    void setActiveVMsAttributeValues(List<String> values) throws IOException;
    List<String> getActiveVMsAttributeValues();
    boolean isActive(String name);

    void setAgentInfos(List<VirtualMachineCurrentState> agentInfos);
    List<AgentInfo> getAgentInfos();

    /**
     * Get all current jobs assigned to VMs. This produces a map with key as the value for VM attribute used to
     * set active VMs. The values of the map are the list of jobs on VM status objects.
     * @return current jobs assigned to VMs.
     */
    Map<String, List<JobsOnVMStatus>> getJobsOnVMs();

    Map<String, AgentClusterAutoScaleRule> getAgentClusterAutoScaleRules();

    class JobOnVMInfo {
        private final String jobId;
        private final int stage;
        private final int workerIndex;
        private final int workerNumber;
        @JsonCreator
        public JobOnVMInfo(@JsonProperty("jobId") String jobId,
                           @JsonProperty("stage") int stage,
                           @JsonProperty("workerIndex") int workerIndex,
                           @JsonProperty("workerNumber") int workerNumber) {
            this.jobId = jobId;
            this.stage = stage;
            this.workerIndex = workerIndex;
            this.workerNumber = workerNumber;
        }
        public String getJobId() {
            return jobId;
        }
        public int getStage() {
            return stage;
        }
        public int getWorkerIndex() {
            return workerIndex;
        }
        public int getWorkerNumber() {
            return workerNumber;
        }
    }
    class JobsOnVMStatus {
        private final String hostname;
        private final String attributeValue;
        private final List<JobOnVMInfo> jobs;
        @JsonCreator
        public JobsOnVMStatus(@JsonProperty("hostname") String hostname,
                              @JsonProperty("attributeValue") String attributeValue) {
            this.hostname = hostname;
            this.attributeValue = attributeValue;
            this.jobs = new ArrayList<>();
        }
        @JsonIgnore
        void addJob(JobOnVMInfo job) {
            jobs.add(job);
        }
        public String getHostname() {
            return hostname;
        }
        public String getAttributeValue() {
            return attributeValue;
        }
        public List<JobOnVMInfo> getJobs() {
            return jobs;
        }
    }

    class AgentInfo {
        private final String name;
        private final double availableCpus;
        private final double availableMemory;
        private final double availableDisk;
        private final int availableNumPorts;
        private final Map<String, Double> scalars;
        private final Map<String, String> attributes;
        private final Set<String> resourceSets;
        private final String disabledUntil;

        @JsonCreator
        public AgentInfo(@JsonProperty("name") String name,
                         @JsonProperty("availableCpus") double availableCpus,
                         @JsonProperty("availableMemory") double availableMemory,
                         @JsonProperty("availableDisk") double availableDisk,
                         @JsonProperty("availableNumPorts") int availableNumPorts,
                         @JsonProperty("scalars") Map<String, Double> scalars,
                         @JsonProperty("attributes") Map<String, String> attributes,
                         @JsonProperty("resourceSets") Set<String> resourceSets,
                         @JsonProperty("disabledUntil") String disabledUntil) {
            this.name = name;
            this.availableCpus = availableCpus;
            this.availableMemory = availableMemory;
            this.availableDisk = availableDisk;
            this.availableNumPorts = availableNumPorts;
            this.scalars = scalars;
            this.attributes = attributes;
            this.resourceSets = resourceSets;
            this.disabledUntil = disabledUntil;
        }

        public String getName() {
            return name;
        }

        public double getAvailableCpus() {
            return availableCpus;
        }

        public double getAvailableMemory() {
            return availableMemory;
        }

        public double getAvailableDisk() {
            return availableDisk;
        }

        public int getAvailableNumPorts() {
            return availableNumPorts;
        }

        public Map<String, Double> getScalars() {
            return scalars;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public Set<String> getResourceSets() {
            return resourceSets;
        }

        public String getDisabledUntil() {
            return disabledUntil;
        }
    }

    class AgentClusterAutoScaleRule {
        private final String name;
        private final long cooldownSecs;
        private final int minIdle;
        private final int maxIdle;
        private final int minSize;
        private final int maxSize;
        @JsonCreator
        public AgentClusterAutoScaleRule(@JsonProperty("name") final String name,
                           @JsonProperty("cooldownSecs") final long cooldownSecs,
                           @JsonProperty("minIdle") final int minIdle,
                           @JsonProperty("maxIdle") final int maxIdle,
                           @JsonProperty("minSize") final int minSize,
                           @JsonProperty("maxSize") final int maxSize) {

            this.name = name;
            this.cooldownSecs = cooldownSecs;
            this.minIdle = minIdle;
            this.maxIdle = maxIdle;
            this.minSize = minSize;
            this.maxSize = maxSize;
        }

        public String getName() {
            return name;
        }

        public long getCooldownSecs() {
            return cooldownSecs;
        }

        public int getMinIdle() {
            return minIdle;
        }

        public int getMaxIdle() {
            return maxIdle;
        }

        public int getMinSize() {
            return minSize;
        }

        public int getMaxSize() {
            return maxSize;
        }
    }
}
