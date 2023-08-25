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

package io.mantisrx.server.master.store;

import io.mantisrx.common.Label;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.runtime.parameter.Parameter;
import java.net.URL;
import java.util.Collection;
import java.util.List;


public interface MantisJobMetadata {

    long DEFAULT_STARTED_AT_EPOCH = 0;

    String getJobId();

    String getName();

    String getUser();

    long getSubmittedAt();

    long getStartedAt();

    URL getJarUrl();

    JobSla getSla();

    long getSubscriptionTimeoutSecs();

    MantisJobState getState();

    List<Parameter> getParameters();

    List<Label> getLabels();

    Collection<? extends MantisStageMetadata> getStageMetadata();

    int getNumStages();

    MantisStageMetadata getStageMetadata(int stageNum);

    MantisWorkerMetadata getWorkerByIndex(int stageNumber, int workerIndex) throws InvalidJobException;

    MantisWorkerMetadata getWorkerByNumber(int workerNumber) throws InvalidJobException;

    AutoCloseable obtainLock();

    int getNextWorkerNumberToUse();

    WorkerMigrationConfig getMigrationConfig();

    long getHeartbeatIntervalSecs();

    long getWorkerTimeoutSecs();
}
