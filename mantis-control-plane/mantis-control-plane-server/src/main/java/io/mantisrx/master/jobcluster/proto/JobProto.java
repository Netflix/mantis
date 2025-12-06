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
import io.mantisrx.master.jobcluster.job.worker.IMantisWorkerMetadata;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.resourcecluster.proto.MantisResourceClusterReservationProto.ReservationPriority.PriorityType;
import java.time.Instant;
import java.util.List;

public class JobProto {


	public interface JobEvent {
		public String getName();
	}


	public static final class InitJob extends BaseRequest {
		public final ActorRef requstor;
		public final boolean isSubmit;
		public InitJob(ActorRef requestor) {
			this(requestor, true);
		}
		public InitJob(ActorRef requestor, boolean isSubmit) {
            this.requstor = requestor;
            this.isSubmit = isSubmit;
        }

        @Override
        public String toString() {
            return "InitJob{" +
                    "requstor=" + requstor +
                    ", isSubmit=" + isSubmit +
                    ", requestId=" + requestId +
                    '}';
        }
    }

	public static final class JobInitialized extends BaseResponse {
		public final JobId jobId;
		public final ActorRef requestor;

		public JobInitialized(final long requestId,
				final ResponseCode responseCode,
				final String message, JobId jobId, ActorRef requestor) {
			super(requestId, responseCode, message);
			this.jobId = jobId;
			this.requestor = requestor;

		}

        @Override
        public String toString() {
            return "JobInitialized{" +
                    "jobId=" + jobId +
                    ", requestor=" + requestor +
                    ", requestId=" + requestId +
                    ", responseCode=" + responseCode +
                    ", message='" + message + '\'' +
                    '}';
        }
    }

	/////////////////////////////////// JOB Related Messages ///////////////////////////////////////////////



	public static final class RuntimeLimitReached {

	}

	public static final class CheckHeartBeat {
		Instant n = null;
		public CheckHeartBeat() {

		}

		public CheckHeartBeat(Instant now) {
			n = now;
		}

		public Instant getTime() {
		    if(n == null) {
		        return Instant.now();
            } else {
		        return n;
            }
        }


	}

	public static final class SendWorkerAssignementsIfChanged {

    }


	public static final class MigrateDisabledVmWorkersRequest {
		public final Instant time;
		public MigrateDisabledVmWorkersRequest(Instant time) {
			this.time = time;;
		}
		public MigrateDisabledVmWorkersRequest() {
			this(Instant.now());
		}
	}

	/**
	 * Message for delayed reservation requests.
	 * Used when workers need to be queued via reservation system but with a delay (rate limiting).
	 */
	public static final class DelayedReservationRequest {
		public final List<IMantisWorkerMetadata> workerRequests;
		public final PriorityType priorityType;

		public DelayedReservationRequest(List<IMantisWorkerMetadata> workerRequests, PriorityType priorityType) {
			this.workerRequests = workerRequests;
			this.priorityType = priorityType;
		}

		@Override
		public String toString() {
			return "DelayedReservationRequest{" +
					"workerCount=" + (workerRequests != null ? workerRequests.size() : 0) +
					", priorityType=" + priorityType +
					'}';
		}
	}

	/**
	 * Result of a reservation upsert operation, piped back to the JobActor.
	 * Contains either success (Ack) or failure (exception) along with context for retry.
	 */
	public static final class ReservationUpsertResult {
		public final List<IMantisWorkerMetadata> workerRequests;
		public final PriorityType priorityType;
		public final int stageNum;
		public final int attemptNumber;
		public final boolean success;
		public final Throwable error;

		private ReservationUpsertResult(
				List<IMantisWorkerMetadata> workerRequests,
				PriorityType priorityType,
				int stageNum,
				int attemptNumber,
				boolean success,
				Throwable error) {
			this.workerRequests = workerRequests;
			this.priorityType = priorityType;
			this.stageNum = stageNum;
			this.attemptNumber = attemptNumber;
			this.success = success;
			this.error = error;
		}

		public static ReservationUpsertResult success(
				List<IMantisWorkerMetadata> workerRequests,
				PriorityType priorityType,
				int stageNum,
				int attemptNumber) {
			return new ReservationUpsertResult(workerRequests, priorityType, stageNum, attemptNumber, true, null);
		}

		public static ReservationUpsertResult failure(
				List<IMantisWorkerMetadata> workerRequests,
				PriorityType priorityType,
				int stageNum,
				int attemptNumber,
				Throwable error) {
			return new ReservationUpsertResult(workerRequests, priorityType, stageNum, attemptNumber, false, error);
		}

		@Override
		public String toString() {
			return "ReservationUpsertResult{" +
					"workerCount=" + (workerRequests != null ? workerRequests.size() : 0) +
					", priorityType=" + priorityType +
					", stageNum=" + stageNum +
					", attemptNumber=" + attemptNumber +
					", success=" + success +
					", error=" + (error != null ? error.getMessage() : "null") +
					'}';
		}
	}

	/**
	 * Message to trigger a retry of reservation upsert after a delay.
	 * Sent to self via Akka scheduler when a reservation upsert fails.
	 */
	public static final class ReservationRetryRequest {
		public final List<IMantisWorkerMetadata> workerRequests;
		public final PriorityType priorityType;
		public final int stageNum;
		public final int attemptNumber;

		public ReservationRetryRequest(
				List<IMantisWorkerMetadata> workerRequests,
				PriorityType priorityType,
				int stageNum,
				int attemptNumber) {
			this.workerRequests = workerRequests;
			this.priorityType = priorityType;
			this.stageNum = stageNum;
			this.attemptNumber = attemptNumber;
		}

		@Override
		public String toString() {
			return "ReservationRetryRequest{" +
					"workerCount=" + (workerRequests != null ? workerRequests.size() : 0) +
					", priorityType=" + priorityType +
					", stageNum=" + stageNum +
					", attemptNumber=" + attemptNumber +
					'}';
		}
	}

    public static class SelfDestructRequest {
    }
}
