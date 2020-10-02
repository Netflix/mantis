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
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import io.mantisrx.master.api.akka.route.proto.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy actor that receives the StatusEvent messages from the StatusEventBrokerActor and forwards
 * it onto the Websocket connection from the client
 */
public class JobStatusConnectedWSActor extends AbstractActor {

    private final Logger logger = LoggerFactory.getLogger(JobStatusConnectedWSActor.class);
    public static Props props(final String jobId, final ActorRef statusEventBrokerActor) {
        return Props.create(JobStatusConnectedWSActor.class, jobId, statusEventBrokerActor);
    }

    private final String jobId;
    private final ActorRef statusEventBrokerActor;

    public JobStatusConnectedWSActor(final String jobId, final ActorRef statusEventBrokerActor) {
        this.jobId = jobId;
        this.statusEventBrokerActor = statusEventBrokerActor;
    }

    // Proto
    public static class Connected {
        private final ActorRef wsActor;

        public Connected(final ActorRef wsActor) {
            this.wsActor = wsActor;
        }

        public ActorRef getWsActor() {
            return wsActor;
        }

        @Override
        public String toString() {
            return "Connected{" +
                "wsActor=" + wsActor +
                '}';
        }
    }

    // Behavior
    private final Receive waitingBehavior() {
        return receiveBuilder()
            .match(Connected.class, this::onConnected)
            .build();
    }

    private void onConnected(final Connected connectedMsg) {
        logger.info("connected {}", connectedMsg);
        statusEventBrokerActor.tell(new StatusEventBrokerActor.JobStatusRequest(jobId), self());
        getContext().watch(connectedMsg.wsActor);
        Receive connected = connectedBehavior(connectedMsg.wsActor);
        getContext().become(connected);
    }

    private void onTerminated(final Terminated t) {
        logger.info("actor terminated {}", t);
        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private Receive connectedBehavior(final ActorRef wsActor) {
        return receiveBuilder()
            .match(JobStatus.class, js -> {
                logger.debug("writing to WS {}", js);
                wsActor.tell(js, self());
            })
            .match(Terminated.class, t -> onTerminated(t))
            .build();
    }

    @Override
    public Receive createReceive() {
        return waitingBehavior();
    }
}
