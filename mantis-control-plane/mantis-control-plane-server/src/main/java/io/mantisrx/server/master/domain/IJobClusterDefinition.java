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

import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.JobPrincipal;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.parameter.Parameter;
import java.util.List;


public interface IJobClusterDefinition {

    JobOwner getOwner();

    SLA getSLA();

    WorkerMigrationConfig getWorkerMigrationConfig();

    boolean getIsReadyForJobMaster();

    List<JobClusterConfig> getJobClusterConfigs();

    JobClusterConfig getJobClusterConfig();

    String getName();

    String getUser();

    String toString();

    List<Parameter> getParameters();

    List<Label> getLabels();

    enum CronPolicy {KEEP_EXISTING, KEEP_NEW}

    JobPrincipal getJobPrincipal();
}
