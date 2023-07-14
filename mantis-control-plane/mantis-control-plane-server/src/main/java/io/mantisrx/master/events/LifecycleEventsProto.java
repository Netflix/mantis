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

package io.mantisrx.master.events;

import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;

import io.mantisrx.master.jobcluster.WorkerInfoListHolder;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.server.master.domain.JobId;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

public class LifecycleEventsProto {

    public static final class AuditEvent {
        public enum AuditEventType {
            // job cluster events
            JOB_CLUSTER_CREATE,
            JOB_CLUSTER_EXISTS,
            JOB_CLUSTER_FAILURE,
            JOB_CLUSTER_UPDATE,
            JOB_CLUSTER_DELETE,
            JOB_CLUSTER_DISABLED,
            JOB_CLUSTER_ENABLED,
            // job events
            JOB_SUBMIT,
            JOB_START,
            JOB_TERMINATE,
            JOB_SHUTDOWN,
            JOB_DELETE,
            JOB_SCALE_UP,
            JOB_SCALE_DOWN,
            JOB_SCALE_UPDATE,
            JOB_FAILURE,
            // worker events
            WORKER_START,
            WORKER_TERMINATE,
            WORKER_RESUBMIT,
            WORKER_RESUBMITS_LIMIT,
            WORKER_STATUS_HB,
            // agent cluster events
            CLUSTER_SCALE_UP,
            CLUSTER_SCALE_DOWN,
            CLUSTER_ACTIVE_VMS,
            //actor events
            JOB_CLUSTER_ACTOR_CREATE,
            JOB_CLUSTER_ACTOR_TERMINATE,
        }

        private final AuditEventType auditEventType;
        private final String operand;
        private final String data;

        public AuditEvent(AuditEventType auditEventType, String operand, String data) {
            this.auditEventType = auditEventType;
            this.operand = operand;
            this.data = data;
        }
        public AuditEventType getAuditEventType() {
            return auditEventType;
        }
        public String getOperand() {
            return operand;
        }
        public String getData() {
            return data;
        }

        @Override
        public String toString() {
            return "AuditEvent{" +
                "auditEventType=" + auditEventType +
                ", operand='" + operand + '\'' +
                ", data='" + data + '\'' +
                '}';
        }
    }

    @Getter
    @EqualsAndHashCode
    @AllArgsConstructor
    @ToString
    public static class StatusEvent {
        public enum StatusEventType {
            ERROR, WARN, INFO, DEBUG, HEARTBEAT
        }
        protected final StatusEventType statusEventType;
        protected final String message;
        protected final long timestamp;

        public StatusEvent(StatusEventType type, String message) {
            this(type, message, System.currentTimeMillis());
        }
    }

    @Getter
    @ToString
    public static final class WorkerStatusEvent extends StatusEvent {
        private final int stageNum;
        private final WorkerId workerId;
        private final WorkerState workerState;
        private final Optional<String> hostName;

        public WorkerStatusEvent(final StatusEventType type,
                                 final String message,
                                 final int stageNum,
                                 final WorkerId workerId,
                                 final WorkerState workerState) {
            super(type, message);
            this.stageNum = stageNum;
            this.workerId = workerId;
            this.workerState = workerState;
            this.hostName = empty();
        }

        public WorkerStatusEvent(final StatusEventType type,
                                 final String message,
                                 final int stageNum,
                                 final WorkerId workerId,
                                 final WorkerState workerState,
                                 final long ts) {
            super(type, message, ts);
            this.stageNum = stageNum;
            this.workerId = workerId;
            this.workerState = workerState;
            this.hostName = empty();
        }

        public WorkerStatusEvent(final StatusEventType type,
                                 final String message,
                                 final int stageNum,
                                 final WorkerId workerId,
                                 final WorkerState workerState,
                                 final String hostName,
                                 final long ts) {
            super(type, message, ts);
            this.stageNum = stageNum;
            this.workerId = workerId;
            this.workerState = workerState;
            this.hostName = ofNullable(hostName);
        }



        public WorkerStatusEvent(final StatusEventType type,
                                 final String message,
                                 final int stageNum,
                                 final WorkerId workerId,
                                 final WorkerState workerState,
                                 final Optional<String> hostName) {
            super(type, message);
            this.stageNum = stageNum;
            this.workerId = workerId;
            this.workerState = workerState;
            this.hostName = hostName;
        }
    }

    @ToString
    @Getter
    public static final class JobStatusEvent extends StatusEvent {
        private final JobId jobId;
        private final JobState jobState;

        public JobStatusEvent(final StatusEventType type,
                                 final String message,
                                 final JobId jobId,
                                 final JobState jobState) {
            super(type, message);
            this.jobId = jobId;
            this.jobState = jobState;
        }
    }

    @ToString
    public static final class JobClusterStatusEvent extends StatusEvent {
        private final String jobCluster;

        public JobClusterStatusEvent(final StatusEventType type,
                              final String message,
                              final String jobCluster) {
            super(type, message);
            this.jobCluster = jobCluster;
        }

        public String getJobCluster() {
            return jobCluster;
        }
    }

    public static Status from(final StatusEvent ev) {
        Status.TYPE type;
        switch (ev.statusEventType) {
            case INFO:
                type = Status.TYPE.INFO;
                break;
            case WARN:
                type = Status.TYPE.WARN;
                break;
            case DEBUG:
                type = Status.TYPE.DEBUG;
                break;
            case ERROR:
                type = Status.TYPE.ERROR;
                break;
            case HEARTBEAT:
                type = Status.TYPE.HEARTBEAT;
                break;
            default:
                throw new IllegalArgumentException("status event type cannot be translated to Status Type "+ ev.statusEventType.name());
        }

        Status status = new Status("None", -1, -1, -1, Status.TYPE.DEBUG, "Invalid", MantisJobState.Noop);
        if (ev instanceof LifecycleEventsProto.JobStatusEvent) {
            JobStatusEvent jse = (JobStatusEvent) ev;
            status = new Status(jse.jobId.getId(), -1, -1, -1, type, jse.getJobId() + " " + jse.message,
                DataFormatAdapter.convertToMantisJobState(jse.jobState));
        } else if (ev instanceof LifecycleEventsProto.JobClusterStatusEvent) {
            JobClusterStatusEvent jcse = (JobClusterStatusEvent) ev;
            status = new Status(jcse.jobCluster, -1, -1, -1, type, jcse.getJobCluster() + " " + jcse.message,
                MantisJobState.Noop);
        } else if (ev instanceof LifecycleEventsProto.WorkerStatusEvent) {
            WorkerStatusEvent wse = (WorkerStatusEvent) ev;
            status = new Status(wse.workerId.getJobId(), wse.stageNum, wse.workerId.getWorkerIndex(), wse.workerId.getWorkerNum(), type,
                wse.getWorkerId().getId() + " " + wse.message,
                DataFormatAdapter.convertWorkerStateToMantisJobState(wse.workerState));
        }
        return status;
    }

    public static class WorkerListChangedEvent {
        private final WorkerInfoListHolder workerInfoListHolder;

        public WorkerListChangedEvent(WorkerInfoListHolder workerInfoListHolder) {
            this.workerInfoListHolder = workerInfoListHolder;
        }

        public WorkerInfoListHolder getWorkerInfoListHolder() {
            return workerInfoListHolder;
        }

        @Override
        public String toString() {
            return "WorkerListChangedEvent{" +
                    "workerInfoListHolder=" + workerInfoListHolder +
                    '}';
        }
    }


}
