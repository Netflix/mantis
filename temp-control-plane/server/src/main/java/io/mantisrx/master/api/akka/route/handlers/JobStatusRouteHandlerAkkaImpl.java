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

package io.mantisrx.master.api.akka.route.handlers;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.model.ws.Message;
import akka.http.scaladsl.model.ws.TextMessage;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.api.akka.route.proto.JobStatus;
import io.mantisrx.master.events.JobStatusConnectedWSActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobStatusRouteHandlerAkkaImpl implements JobStatusRouteHandler {
    private static final Logger logger = LoggerFactory.getLogger(JobStatusRouteHandlerAkkaImpl.class);
    private final ActorRef statusEventBrokerActor;
    private final ActorSystem actorSystem;

    public JobStatusRouteHandlerAkkaImpl(final ActorSystem actorSystem,
                                         final ActorRef statusEventBrokerActor) {
        this.actorSystem = actorSystem;
        this.statusEventBrokerActor = statusEventBrokerActor;
    }

    /**
     * Based on https://markatta.com/codemonkey/blog/2016/04/18/chat-with-akka-http-websockets/
     * @param jobId job for which job status is requested
     * @return a flow that ignores the incoming messages from the WS client, and
     * creates a akka Source to emit a stream of JobStatus messages to the WS client
     */
    @Override
    public Flow<Message, Message, NotUsed> jobStatus(final String jobId) {
        ActorRef jobStatusConnectedWSActor = actorSystem.actorOf(JobStatusConnectedWSActor.props(jobId, statusEventBrokerActor),
            "JobStatusConnectedWSActor-" + jobId + "-" + System.currentTimeMillis());
        Sink<Message, NotUsed> incomingMessagesIgnored = Flow.<Message>create().to(Sink.ignore());

        Source<Message, NotUsed> backToWebSocket = Source.<JobStatus>actorRef(100, OverflowStrategy.dropHead())
            .mapMaterializedValue((ActorRef outgoingActor) -> {
                jobStatusConnectedWSActor.tell(
                    new JobStatusConnectedWSActor.Connected(outgoingActor),
                    ActorRef.noSender()
                );

                return NotUsed.getInstance();
            })
            .map(js -> new TextMessage.Strict(Jackson.toJson(js)));

        return Flow.fromSinkAndSource(incomingMessagesIgnored, backToWebSocket);
    }
}
