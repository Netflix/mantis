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

package io.mantisrx.master.jobcluster.proto;


import akka.actor.ActorRef;
import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobDefinition;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class JobClusterProto {

    /**
     * This message is sent to a JobCluster Actor from
     * 1. JobClustersManagerActor during bootstrap - in which case the Job Cluster Actor will create and initialize the list of jobs passed in this message
     * 2. JobClustersManagerActor on receiving a CreateJobClusterRequest from the user - in which case the job cluster actor will persist to storage
     * @author njoshi
     *
     */
    public static final class InitializeJobClusterRequest extends BaseRequest {
        public final JobClusterDefinitionImpl jobClusterDefinition;
        public final ActorRef requestor;
        public final String user;
        public final boolean isDisabled;
        public final long lastJobNumber;
        public final boolean createInStore;
        public final List<IMantisJobMetadata> jobList;

        /**
         * Invoked directly during bootstrap
         * @param jobClusterDefinition
         * @param isDisabled
         * @param lastJobNumber
         * @param jobList
         * @param user
         * @param requestor
         * @param createInStore
         */
        public InitializeJobClusterRequest(final JobClusterDefinitionImpl jobClusterDefinition, boolean isDisabled, long lastJobNumber,
                                           List<IMantisJobMetadata> jobList, String user, ActorRef requestor, boolean createInStore) {
            super();
            Preconditions.checkNotNull(jobClusterDefinition, "JobClusterDefn cannot be null");
            this.jobClusterDefinition = jobClusterDefinition;
            this.user = user;
            this.requestor = requestor;
            this.createInStore = createInStore;
            this.isDisabled = isDisabled;
            this.lastJobNumber = lastJobNumber;
            this.jobList = jobList;

        }
        /**
         * Invoked during Job Cluster Creation
         * @param jobClusterDefinition
         * @param user
         * @param requestor
         */
        public InitializeJobClusterRequest(final JobClusterDefinitionImpl jobClusterDefinition, String user, ActorRef requestor) {
            this(jobClusterDefinition, false, 0, Lists.newArrayList(), user, requestor, true);

        }

        @Override
        public String toString() {
            return "InitializeJobClusterRequest{" +
                    "jobClusterDefinition=" + jobClusterDefinition +
                    ", requestor=" + requestor +
                    ", user='" + user + '\'' +
                    ", isDisabled=" + isDisabled +
                    ", lastJobNumber=" + lastJobNumber +
                    ", createInStore=" + createInStore +
                    ", jobList=" + jobList +
                    '}';
        }
    }

    /**
     * Indicates whether a job cluster was initialized successfully
     * Typical failures include unable to write to store
     * @author njoshi
     *
     */
    public static final class InitializeJobClusterResponse extends BaseResponse {
        public final ActorRef requestor;
        public final String jobClusterName;
        public InitializeJobClusterResponse(final long requestId,
                                            final ResponseCode responseCode,
                                            final String message,
                                            final String jobClusterName,
                                            final ActorRef requestor) {
            super(requestId, responseCode, message);
            this.requestor = requestor;
            this.jobClusterName = jobClusterName;
        }
    }


//    public static final class AddArchivedJobs {
//        public final List<IMantisJobMetadata> archivedJobsList;
//        public AddArchivedJobs(List<IMantisJobMetadata> list) {
//            this.archivedJobsList = list;
//        }
//    }




    /**
     * Deletes all records associated with this job cluster in store
     * Terminates cluster actor
     *
     * Only allowed if there are no jobs currently running.
     * @author njoshi
     *
     */

    public static final class DeleteJobClusterRequest extends BaseRequest {
        public final String jobClusterName;
        public final String user;
        public final ActorRef requestingActor;
        public DeleteJobClusterRequest(final String user, final String name, final ActorRef requestor) {
            super();
            this.jobClusterName = name;
            this.user = user;
            this.requestingActor = requestor;
        }
    }

    /**
     * Whether the delete was successful
     * @author njoshi
     *
     */
    public static final class DeleteJobClusterResponse extends BaseResponse {

        public final ActorRef requestingActor;
        public final String clusterName;
        public DeleteJobClusterResponse(long requestId, ResponseCode responseCode, String message, ActorRef requestingActor, String clusterName) {
            super(requestId, responseCode, message);
            this.requestingActor = requestingActor;
            this.clusterName = clusterName;
        }

        public ActorRef getRequestingActor() {
            return requestingActor;
        }

        public String getClusterName() {
            return clusterName;
        }
    }

    public static final class KillJobRequest  extends BaseRequest {
        public final JobId jobId;
        public final String reason;
        public final JobCompletedReason jobCompletedReason;
        public final String user;
        public final ActorRef requestor;

        public KillJobRequest(final JobId jobId,
                              final String reason,
                              final JobCompletedReason jobCompletedReason,
                              final String user,
                              final ActorRef requestor) {
            super();
            this.jobId = jobId;
            this.reason = reason;
            this.jobCompletedReason = jobCompletedReason;
            this.user = user;
            this.requestor = requestor;
        }

        @Override
        public String toString() {
            return "KillJobRequest [jobId=" + jobId + ", reason=" + reason + ", user=" + user + ", requestor="
                + requestor + "]";
        }
    }

    public static final class KillJobResponse extends BaseResponse {
        public final JobId jobId;
        public final ActorRef requestor;
        public final JobState state;
        public final String user;
        public final IMantisJobMetadata jobMetadata;
        public KillJobResponse(long requestId, ResponseCode responseCode, JobState state, String message, JobId jobId, IMantisJobMetadata jobMeta, String user,
                               final ActorRef requestor) {
            super(requestId, responseCode, message);
            this.jobId = jobId;
            this.requestor = requestor;
            this.state = state;
            this.user = user;
            this.jobMetadata = jobMeta;
        }

        @Override
        public String toString() {
            return "KillJobResponse{" +
                    "jobId=" + jobId +
                    ", requestor=" + requestor +
                    ", state=" + state +
                    ", user='" + user + '\'' +
                    ", jobMetadata=" + jobMetadata +
                    '}';
        }
    }

    public static final class JobStartedEvent {
        public final JobId jobid;
        public JobStartedEvent(JobId jobId) {
            this.jobid = jobId;
        }
        @Override
        public String toString() {
            return "JobStartedEvent [jobid=" + jobid + "]";
        }
    }

    public static final class EnforceSLARequest {
        public final Instant timeOfEnforcement;
        public final Optional<JobDefinition> jobDefinitionOp;
        public EnforceSLARequest() {
            this(Instant.now(),Optional.empty());
        }
        public EnforceSLARequest(Instant now) {
            this( now, Optional.empty());
        }

        public EnforceSLARequest(Instant now, Optional<JobDefinition> jobDefnOp) {
            this.timeOfEnforcement = now;
            this.jobDefinitionOp = jobDefnOp;

        }
    }

    public static final class BookkeepingRequest {
        public final Instant time;
        public BookkeepingRequest(Instant time) {
            this.time = time;;
        }
        public BookkeepingRequest() {
            this(Instant.now());
        }

    }

    public static final class TriggerCronRequest {
        public final Instant time;
        public TriggerCronRequest(Instant time) {
            this.time = time;;
        }
        public TriggerCronRequest() {
            this(Instant.now());
        }

    }

}
