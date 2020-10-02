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

package io.mantisrx.server.master;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.mantisrx.common.Label;
import io.mantisrx.runtime.MantisJobDefinition;
import io.mantisrx.runtime.NamedJobDefinition;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.server.master.store.InvalidJobException;
import io.mantisrx.server.master.store.InvalidNamedJobException;
import io.mantisrx.server.master.store.MantisJobStore;
import io.mantisrx.server.master.store.NamedJob;
import io.mantisrx.server.master.store.NamedJobDeleteException;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func2;


public interface MantisJobOperations {

    NamedJob createNamedJob(NamedJobDefinition namedJobDefinition)
            throws InvalidNamedJobException;

    NamedJob updateNamedJar(NamedJobDefinition namedJobDefinition, boolean createIfNeeded) throws InvalidNamedJobException;

    NamedJob quickUpdateNamedJob(String user, String name, URL jobJar, String version) throws InvalidNamedJobException;

    void updateSla(String user, String name, NamedJob.SLA sla, boolean forceEnable) throws InvalidNamedJobException;

    /**
     * Update the Labels associated with the Job cluster. This complete replaces any existing labels.
     *
     * @param user   : submitter
     * @param name   : Job cluster name
     * @param labels List of Label objects
     *
     * @throws InvalidNamedJobException
     */
    void updateLabels(String user, String name, List<Label> labels) throws InvalidNamedJobException;

    void updateMigrateStrategy(String user, String name, WorkerMigrationConfig migrationConfig) throws InvalidNamedJobException;

    String quickSubmit(String jobName, String user) throws InvalidNamedJobException, InvalidJobException;

    Optional<NamedJob> getNamedJob(String name);

    void deleteNamedJob(String name, String user) throws NamedJobDeleteException;

    void disableNamedJob(String name, String user) throws InvalidNamedJobException;

    void enableNamedJob(String name, String user) throws InvalidNamedJobException;

    MantisJobStatus submit(MantisJobDefinition jobDefinition);

    boolean deleteJob(String jobId) throws IOException;

    void killJob(String user, String jobId, String reason);

    void terminateJob(String jobId);

    Observable<MantisJobStatus> jobs();

    MantisJobStatus status(String jobId);

    Func2<MantisJobStore, Map<String, MantisJobDefinition>, Collection<NamedJob>> getJobsInitializer();

    Collection<MantisJobMgr> getAllJobMgrs();

    Optional<MantisJobMgr> getJobMgr(String jobId);

    Action1<String> getSlaveDisabler();

    Action1<String> getSlaveEnabler();

    void setReady();
}
