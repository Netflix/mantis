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

import org.apache.pekko.actor.ActorRef;
import io.mantisrx.server.master.domain.JobId;
import java.time.Instant;

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


    public static class SelfDestructRequest {
    }
}
