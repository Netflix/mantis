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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.parameter.Parameter;


public class JobClusterMetadata {

    private final String name;
    private final List<Jar> jars;
    private final JobOwner owner;
    private final SLA sla;
    private final List<Parameter> parameters;
    private final boolean isReadyForJobMaster;
    private final boolean disabled;
    private final WorkerMigrationConfig migrationConfig;
    private final List<Label> labels;
    private final AtomicLong lastJobCount = new AtomicLong(0);

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobClusterMetadata(@JsonProperty("name") String name,
                              @JsonProperty("jars") List<Jar> jars,
                              @JsonProperty("sla") SLA sla,
                              @JsonProperty("parameters") List<Parameter> parameters,
                              @JsonProperty("owner") JobOwner owner,
                              @JsonProperty("lastJobCount") long lastJobCount,
                              @JsonProperty("disabled") boolean disabled,
                              @JsonProperty("isReadyForJobMaster") boolean isReadyForJobMaster,
                              @JsonProperty("migrationConfig") WorkerMigrationConfig migrationConfig,
                              @JsonProperty("labels") List<Label> labels) {
        this.name = name;
        this.jars = Optional.ofNullable(jars).orElse(new ArrayList<>());
        this.sla = sla;
        this.parameters = Optional.ofNullable(parameters).orElse(new ArrayList<>());
        this.isReadyForJobMaster = isReadyForJobMaster;
        this.owner = owner;
        this.migrationConfig = migrationConfig;
        this.labels = labels;
        this.disabled = disabled;
        this.lastJobCount.set(lastJobCount);
    }

    public String getName() {
        return name;
    }

    public List<Jar> getJars() {
        return jars;
    }

    public JobOwner getOwner() {
        return owner;
    }

    public SLA getSla() {
        return sla;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public boolean isReadyForJobMaster() {
        return isReadyForJobMaster;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public WorkerMigrationConfig getMigrationConfig() {
        return migrationConfig;
    }

    public List<Label> getLabels() {
        return labels;
    }

    public AtomicLong getLastJobCount() {
        return lastJobCount;
    }
}
