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

import io.mantisrx.server.master.resourcecluster.TaskExecutorID;
import io.mantisrx.server.master.resourcecluster.TaskExecutorRegistration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import rx.Observable;


public class NoopStorageProvider implements MantisStorageProvider {

    @Override
    public void storeNewJob(MantisJobMetadataWritable jobMetadata) throws JobAlreadyExistsException, IOException {
    }

    @Override
    public void updateJob(MantisJobMetadataWritable jobMetadata) throws InvalidJobException, IOException {
    }

    @Override
    public void deleteJob(String jobId) throws InvalidJobException, IOException {
    }

    @Override
    public void archiveJob(String jobId) throws IOException {
    }

    @Override
    public void storeMantisStage(MantisStageMetadataWritable msmd) throws IOException {
    }

    @Override
    public void updateMantisStage(MantisStageMetadataWritable msmd) throws IOException {
    }

    @Override
    public void storeWorker(MantisWorkerMetadataWritable workerMetadata) throws IOException {
    }

    @Override
    public void storeWorkers(String jobId, List<MantisWorkerMetadataWritable> workers) throws IOException {
    }

    @Override
    public void storeAndUpdateWorkers(MantisWorkerMetadataWritable worker1, MantisWorkerMetadataWritable worker2) throws InvalidJobException, IOException {
    }

    @Override
    public void updateWorker(MantisWorkerMetadataWritable mwmd) throws IOException {
    }

    @Override
    public List<MantisJobMetadataWritable> initJobs() throws IOException {
        return new ArrayList<>();
    }

    @Override
    public Observable<MantisJobMetadata> initArchivedJobs() {
        return Observable.empty();
    }

    @Override
    public List<NamedJob> initNamedJobs() throws IOException {
        return new ArrayList<>();
    }

    @Override
    public Observable<NamedJob.CompletedJob> initNamedJobCompletedJobs() throws IOException {
        return Observable.empty();
    }

    @Override
    public void archiveWorker(MantisWorkerMetadataWritable mwmd) throws IOException {
    }

    @Override
    public List<MantisWorkerMetadataWritable> getArchivedWorkers(String jobid) throws IOException {
        return new ArrayList<>();
    }

    @Override
    public void storeNewNamedJob(NamedJob namedJob) throws JobNameAlreadyExistsException, IOException {
    }

    @Override
    public void updateNamedJob(NamedJob namedJob) throws InvalidNamedJobException, IOException {
    }

    @Override
    public boolean deleteNamedJob(String name) throws IOException {
        return true;
    }

    @Override
    public void storeCompletedJobForNamedJob(String name, NamedJob.CompletedJob job) {
        return;
    }

    @Override
    public MantisJobMetadataWritable loadArchivedJob(String jobId) throws IOException {
        return null;
    }

    @Override
    public void removeCompledtedJobForNamedJob(String name, String jobId) throws IOException {
        return;
    }

    @Override
    public void setActiveVmAttributeValuesList(List<String> vmAttributesList) throws IOException {}

    @Override
    public List<String> initActiveVmAttributeValuesList() {
        return null;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public TaskExecutorRegistration getTaskExecutorFor(TaskExecutorID taskExecutorID) throws IOException {
        return null;
    }

    @Override
    public void storeNewTaskExecutor(TaskExecutorRegistration registration) {

    }
}
