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

import akka.actor.ActorRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusEventSubscriberAkkaImpl implements StatusEventSubscriber {
    private static final Logger logger = LoggerFactory.getLogger(StatusEventSubscriberAkkaImpl.class);

    private final ActorRef statusEventBrokerActor;

    public StatusEventSubscriberAkkaImpl(final ActorRef statusEventBrokerActor) {
        this.statusEventBrokerActor = statusEventBrokerActor;
    }

    @Override
    public void process(final LifecycleEventsProto.StatusEvent statusEvent) {
        logger.debug("[STATUS] {}", statusEvent);
        statusEventBrokerActor.tell(statusEvent, ActorRef.noSender());
    }
}
