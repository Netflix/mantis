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

package io.mantisrx.master.jobcluster.job;

import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.server.master.domain.Costs;
import io.mantisrx.server.master.store.MantisJobMetadataWritable;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonFilter;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URL;
import java.util.List;

@JsonFilter("jobMetadata")
public class FilterableMantisJobMetadataWritable extends MantisJobMetadataWritable {
    private final Costs costs;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public FilterableMantisJobMetadataWritable(@JsonProperty("jobId") String jobId,
                                               @JsonProperty("name") String name,
                                               @JsonProperty("user") String user,
                                               @JsonProperty("submittedAt") long submittedAt,
                                               @JsonProperty("startedAt") long startedAt,
                                               @JsonProperty("jarUrl") URL jarUrl,
                                               @JsonProperty("numStages") int numStages,
                                               @JsonProperty("sla") JobSla sla,
                                               @JsonProperty("state") MantisJobState state,
                                               @JsonProperty("workerTimeoutSecs") long workerTimeoutSecs,
                                               @JsonProperty("heartbeatIntervalSecs") long heartbeatIntervalSecs,
                                               @JsonProperty("subscriptionTimeoutSecs") long subscriptionTimeoutSecs,
                                               @JsonProperty("parameters") List<Parameter> parameters,
                                               @JsonProperty("nextWorkerNumberToUse") int nextWorkerNumberToUse,
                                               @JsonProperty("migrationConfig") WorkerMigrationConfig migrationConfig,
                                               @JsonProperty("labels") List<Label> labels,
                                               @JsonProperty("costs") Costs costs) {
        super(jobId, name, user, submittedAt, startedAt, jarUrl, numStages, sla, state, workerTimeoutSecs,
            heartbeatIntervalSecs, subscriptionTimeoutSecs, parameters, nextWorkerNumberToUse, migrationConfig, labels);
        this.costs = costs;
    }

    public Costs getCosts() {
        return costs;
    }
}
