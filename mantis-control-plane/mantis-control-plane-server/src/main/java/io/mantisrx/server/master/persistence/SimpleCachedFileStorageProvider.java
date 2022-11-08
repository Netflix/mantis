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
import io.mantisrx.server.master.store.KeyValueStorageProvider;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simple File based storage provider. Intended mainly as a sample implementation for
 * {@link IMantisStorageProvider} interface. This implementation is complete in its functionality, but, isn't
 * expected to be scalable or performant for production loads.
 * <P>This implementation uses <code>/tmp/MantisSpool/</code> as the spool directory. The directory is created
 * if not present already. It will fail only if either a file with that name exists or if a directory with that
 * name exists but isn't writable.</P>
 */
public class SimpleCachedFileStorageProvider extends KeyValueAwareMantisStorageProvider {
    private static final Logger logger = LoggerFactory.getLogger(SimpleCachedFileStorageProvider.class);
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

    public SimpleCachedFileStorageProvider(boolean actualStorageProvider) {
        this((actualStorageProvider) ? new io.mantisrx.server.master.store.SimpleCachedFileStorageProvider() : KeyValueStorageProvider.NO_OP,
            noopEventPublisher);
    }

    public SimpleCachedFileStorageProvider(KeyValueStorageProvider sprovider, LifecycleEventPublisher publisher) {
        super(sprovider, publisher);
    }

    public SimpleCachedFileStorageProvider(io.mantisrx.server.master.store.SimpleCachedFileStorageProvider sprovider) {
        this(sprovider, noopEventPublisher);
    }

    public SimpleCachedFileStorageProvider(File stateDirectory, boolean eventPublisher) {
        this(new io.mantisrx.server.master.store.SimpleCachedFileStorageProvider(stateDirectory), noopEventPublisher);
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
