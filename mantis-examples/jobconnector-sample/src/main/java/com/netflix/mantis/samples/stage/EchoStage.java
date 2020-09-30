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

package com.netflix.mantis.samples.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.ScalarComputation;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


/**
 * A simple stage that extracts data from the incoming {@link MantisServerSentEvent} and echoes it.
 */
@Slf4j
public class EchoStage implements ScalarComputation<MantisServerSentEvent,String> {
    private static final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Observable<String> call(Context context, Observable<MantisServerSentEvent> eventsO) {
        return eventsO
                .map(MantisServerSentEvent::getEventAsString)
                .map((event) -> {
                    log.info("Received: {}", event);
                    return event;
                });
    }

    @Override
    public void init(Context context) {

    }

    public static ScalarToScalar.Config<MantisServerSentEvent,String> config(){
        return new ScalarToScalar.Config<MantisServerSentEvent,String>()
                .codec(Codecs.string());
    }

}
