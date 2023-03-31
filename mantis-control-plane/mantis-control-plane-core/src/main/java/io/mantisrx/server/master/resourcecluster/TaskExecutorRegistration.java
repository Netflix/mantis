/*
 * Copyright 2022 Netflix, Inc.
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
package io.mantisrx.server.master.resourcecluster;

import io.mantisrx.common.WorkerConstants;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

/**
 * Data structure used at the time of registration by the task executor.
 * Different fields help identify the task executor in different dimensions.
 */
@Builder
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
@ToString
@EqualsAndHashCode
public class TaskExecutorRegistration {
    @NonNull
    TaskExecutorID taskExecutorID;

    @NonNull
    ClusterID clusterID;

    // RPC address that's used to talk to the task executor
    @NonNull
    String taskExecutorAddress;

    // host name of the task executor
    @NonNull
    String hostname;

    // ports used by the task executor for various purposes.
    @NonNull
    WorkerPorts workerPorts;

    // machine information identifies the cpu/mem/disk/network resources of the task executor.
    @NonNull
    MachineDefinition machineDefinition;

    /** custom attributes describing the task executor
    * [Note] all keys/values need to be save as lower-case to avoid mismatch.
    * TODO make this field non-null once no back-compat required.
    **/
    Map<String, String> taskExecutorAttributes;

    @JsonCreator
    public TaskExecutorRegistration(
        @JsonProperty("taskExecutorID") TaskExecutorID taskExecutorID,
        @JsonProperty("clusterID") ClusterID clusterID,
        @JsonProperty("taskExecutorAddress") String taskExecutorAddress,
        @JsonProperty("hostname") String hostname,
        @JsonProperty("workerPorts") WorkerPorts workerPorts,
        @JsonProperty("machineDefinition") MachineDefinition machineDefinition,
        @JsonProperty("taskExecutorAttributes") Map<String, String> taskExecutorAttributes) {
        this.taskExecutorID = taskExecutorID;
        this.clusterID = clusterID;
        this.taskExecutorAddress = taskExecutorAddress;
        this.hostname = hostname;
        this.workerPorts = workerPorts;
        this.machineDefinition = machineDefinition;
        this.taskExecutorAttributes = (taskExecutorAttributes == null) ? ImmutableMap.of() : taskExecutorAttributes;
    }

    /**
     * Check if all given attributes have a match in taskExecutorAttributes.
     * [Note] all keys/values in taskExecutorAttributes are lower-case and
     * requiredAttributes will be evaluated case-insensitive.
     */
    public boolean containsAttributes(Map<String, String> requiredAttributes) {
        for (Map.Entry<String, String> kv : requiredAttributes.entrySet()) {
            String k = kv.getKey().toLowerCase();
            if (this.taskExecutorAttributes.containsKey(k) &&
                this.taskExecutorAttributes.get(k).equalsIgnoreCase(kv.getValue())) {
                continue;
            }

            // handle back compat on case-sensitive registrations.
            if (this.taskExecutorAttributes.containsKey(kv.getKey()) &&
                this.taskExecutorAttributes.get(kv.getKey()).equalsIgnoreCase(kv.getValue())) {
                continue;
            }

            return false;
        }
        return true;
    }

    @JsonIgnore
    public Optional<ContainerSkuID> getTaskExecutorContainerDefinitionId() {
        // handle back compat on key case insensitivity.
        String containerDefIdLower = WorkerConstants.WORKER_CONTAINER_DEFINITION_ID.toLowerCase();
        if (this.taskExecutorAttributes.containsKey(containerDefIdLower)) {
            return Optional.ofNullable(ContainerSkuID.of(this.getTaskExecutorAttributes().get(containerDefIdLower)));
        }

        if (this.taskExecutorAttributes.containsKey(WorkerConstants.WORKER_CONTAINER_DEFINITION_ID)) {
            return Optional.ofNullable(
                ContainerSkuID.of(
                    this.getTaskExecutorAttributes().get(WorkerConstants.WORKER_CONTAINER_DEFINITION_ID)));
        }

        return Optional.empty();
    }

    @JsonIgnore
    public Optional<String> getAttributeByKey(String attributeKey) {
        if (this.taskExecutorAttributes.containsKey(attributeKey.toLowerCase())) {
            return Optional.ofNullable(this.getTaskExecutorAttributes().get(attributeKey.toLowerCase()));
        }

        if (this.taskExecutorAttributes.containsKey(attributeKey)) {
            return Optional.ofNullable(this.getTaskExecutorAttributes().get(attributeKey));
        }

        return Optional.empty();
    }
}
