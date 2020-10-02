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

package io.mantisrx.master;

import akka.actor.AbstractActor;
import akka.actor.DeadLetter;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeadLetterActor extends AbstractActor {

    private final Logger log = LoggerFactory.getLogger(DeadLetterActor.class);

    private final Metrics metrics;
    private final Counter numDeadLetterMsgs;

    public DeadLetterActor() {
        Metrics m = new Metrics.Builder()
                .id("DeadLetterActor")
                .addCounter("numDeadLetterMsgs")
                .build();
        this.metrics = MetricsRegistry.getInstance().registerAndGet(m);
        this.numDeadLetterMsgs = metrics.getCounter("numDeadLetterMsgs");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DeadLetter.class, msg -> {
                    this.numDeadLetterMsgs.increment();
                    String m = msg.message().toString();
                    log.info("Dead Letter from {} to {} msg:{}", msg.sender(), msg.recipient(),
                            m.substring(0, Math.min(250, m.length() - 1)));
                })
                .build();
    }
}