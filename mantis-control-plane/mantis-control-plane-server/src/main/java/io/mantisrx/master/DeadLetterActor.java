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
import io.mantisrx.common.JsonSerializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeadLetterActor extends AbstractActor {

    private final Counter numDeadLetterMsgs;
    private final JsonSerializer serializer;
    private final MeterRegistry meterRegistry;

    public DeadLetterActor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        numDeadLetterMsgs = meterRegistry.counter("DeadLetterActor_numDeadLetterMsgs");
        this.serializer = new JsonSerializer();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DeadLetter.class, msg -> {
                    this.numDeadLetterMsgs.increment();
                    String m = toString(msg.message());
                    log.error("Dead Letter from {} to {} msg:{}", msg.sender(), msg.recipient(),
                            m.substring(0, Math.min(250, m.length() - 1)));
                })
                .build();
    }

    private String toString(Object o) {
        try {
            return serializer.toJson(o);
        } catch (Exception e) {
            log.error("Failed to serialize {}", o, e);
            return o.toString();
        }
    }
}
