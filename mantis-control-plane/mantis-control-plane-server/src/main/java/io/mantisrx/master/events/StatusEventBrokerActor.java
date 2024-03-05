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

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.dispatch.BoundedMessageQueueSemantics;
import akka.dispatch.RequiresMessageQueue;
import io.mantisrx.master.api.akka.route.proto.JobStatus;
import io.mantisrx.master.jobcluster.job.JobState;
import io.mantisrx.server.core.Status;
import io.mantisrx.shaded.com.google.common.collect.EvictingQueue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobStatus Broker that receives StatusEvents from all actors and demultiplexes to client connections interested in
 * events for a specific jobId
 */
public class StatusEventBrokerActor extends AbstractActor
    implements RequiresMessageQueue<BoundedMessageQueueSemantics> {

    private final Logger logger = LoggerFactory.getLogger(StatusEventBrokerActor.class);
    private final Map<String, Set<ActorRef>>  jobIdToActorMap = new HashMap<>();
    private final Map<ActorRef, String>  actorToJobIdMap = new HashMap<>();

    private final Map<String, EvictingQueue<Status>> jobIdToStatusEventsBuf = new HashMap<>();
    public static final int MAX_STATUS_HISTORY_PER_JOB = 100;

    public static Props props() {
        return Props.create(StatusEventBrokerActor.class);
    }

    public StatusEventBrokerActor() {
    }


    public static class JobStatusRequest {
        private final String jobId;

        public JobStatusRequest(final String jobId) {
            this.jobId = jobId;
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final JobStatusRequest that = (JobStatusRequest) o;
            return Objects.equals(jobId, that.jobId);
        }

        @Override
        public int hashCode() {

            return Objects.hash(jobId);
        }

        @Override
        public String toString() {
            return "JobStatusRequest{" +
                "jobId='" + jobId + '\'' +
                '}';
        }
    }

    private void onJobStatusRequest(final JobStatusRequest jsr) {
        logger.debug("got request {}", jsr);
        ActorRef sender = sender();
        jobIdToActorMap.computeIfAbsent(jsr.jobId, (jobId) -> new HashSet<>());
        jobIdToActorMap.get(jsr.jobId).add(sender);
        actorToJobIdMap.put(sender, jsr.jobId);
        getContext().watch(sender);
        // replay buffered status events on new connection
        EvictingQueue<Status> statusEventsBuf = jobIdToStatusEventsBuf.get(jsr.jobId);
        if (statusEventsBuf != null) {
            statusEventsBuf.forEach(se -> sender.tell(new JobStatus(se), ActorRef.noSender()));
        }
    }

    private void cleanupIfTerminalState(final LifecycleEventsProto.StatusEvent se) {
        if (se instanceof LifecycleEventsProto.JobStatusEvent) {
            LifecycleEventsProto.JobStatusEvent jse = (LifecycleEventsProto.JobStatusEvent) se;
            if (JobState.isTerminalState(jse.getJobState())) {
                jobIdToStatusEventsBuf.remove(jse.getJobId());
            }
        }
    }

    // sends JobStatus messages to active connections by jobId
    private void onStatusEvent(final LifecycleEventsProto.StatusEvent se) {
        Status status = LifecycleEventsProto.from(se);
        String jobId = status.getJobId();

        // add Status to job event history
        jobIdToStatusEventsBuf
            .computeIfAbsent(jobId, (j) -> EvictingQueue.create(MAX_STATUS_HISTORY_PER_JOB))
            .add(status);

        cleanupIfTerminalState(se);
        Set<ActorRef> jobStatusActiveConnections = jobIdToActorMap.get(jobId);
        if (jobStatusActiveConnections != null && !jobStatusActiveConnections.isEmpty()) {
            logger.debug("Sending job status {}", se);
            jobStatusActiveConnections.forEach(connActor -> connActor.tell(new JobStatus(status), self()));
        } else {
            logger.debug("Job status dropped, no active subscribers for {}", jobId);
        }
    }

    private void onTerminated(final Terminated t) {
        logger.info("actor terminated {}", t);
        ActorRef terminatedActor = t.actor();
        String jobId = actorToJobIdMap.get(terminatedActor);
        if (jobId != null) {
            jobIdToActorMap.get(jobId).remove(terminatedActor);
        }
        actorToJobIdMap.remove(terminatedActor);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JobStatusRequest.class, jsr -> onJobStatusRequest(jsr))
            .match(LifecycleEventsProto.StatusEvent.class, js -> onStatusEvent(js))
            .match(Terminated.class, t -> onTerminated(t))
            .build();
    }
}
