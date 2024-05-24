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

package io.mantisrx.server.master.persistence;

import io.mantisrx.master.events.LifecycleEventPublisher;
import io.mantisrx.master.events.LifecycleEventsProto;
import io.mantisrx.master.jobcluster.IJobClusterMetadata;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.server.master.store.FileBasedStore;
import io.mantisrx.server.master.store.NoopStore;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simple File based storage provider. Intended mainly as a sample implementation for
 * {@link IMantisPersistenceProvider} interface. This implementation is complete in its functionality, but, isn't
 * expected to be scalable or performant for production loads.
 * <p>This implementation is mainly for testing.</p>
 */
public class FileBasedPersistenceProvider extends KeyValueBasedPersistenceProvider {
    private static final Logger logger = LoggerFactory.getLogger(FileBasedPersistenceProvider.class);
    private static final LifecycleEventPublisher noopEventPublisher = new LifecycleEventPublisher() {
        @Override
        public void publishAuditEvent(LifecycleEventsProto.AuditEvent auditEvent) {
        }

        @Override
        public void publishStatusEvent(LifecycleEventsProto.StatusEvent statusEvent) {
        }

        @Override
        public void publishWorkerListChangedEvent(LifecycleEventsProto.WorkerListChangedEvent workerListChangedEvent) {
        }
    };

    public FileBasedPersistenceProvider(boolean actualStorageProvider) {
        this((actualStorageProvider) ? new FileBasedStore() : NoopStore.noop(),
            noopEventPublisher);
    }

    public FileBasedPersistenceProvider(IKeyValueStore sprovider, LifecycleEventPublisher publisher) {
        super(sprovider, publisher);
    }

    public FileBasedPersistenceProvider(FileBasedStore sprovider) {
        this(sprovider, noopEventPublisher);
    }

    public FileBasedPersistenceProvider(File stateDirectory, boolean eventPublisher) {
        this(new FileBasedStore(stateDirectory), noopEventPublisher);
    }

    Optional<IJobClusterMetadata> loadJobCluster(String clusterName) throws IOException {
        return loadAllJobClusters().stream()
            .filter(jc -> clusterName.equals(jc.getJobClusterDefinition().getName()))
            .findFirst();
    }

    Optional<IMantisJobMetadata> loadActiveJob(String jobId) throws IOException {
        return loadAllJobs().stream()
            .filter(job -> jobId.equals(job.getJobId().getId()))
            .findFirst();

    }
}
