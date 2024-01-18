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

package com.netflix.mantis.examples.mantispublishsample;

import com.google.inject.Inject;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.EventPublisher;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;


/**
 * A simple example that uses Guice to inject the {@link EventPublisher}  part of the mantis-publish library
 * to send events to Mantis.
 *
 * The mantis-publish library provides on-demand source side filtering via MQL. When a user publishes
 * events via this library the events may not be actually shipped to Mantis. A downstream consumer needs
 * to first register a query and the query needs to match events published by the user.
 *
 */
@Slf4j
public class SampleDataPublisher implements IDataPublisher {
    @Inject
    EventPublisher publisher;
    @Inject
    DataGenerator dataGenerator;

    /**
     * Generates random events at a fixed rate and publishes them to the mantis-publish library.
     * Here the events are published to the defaultStream.
     */
    @Override
    public void generateAndSendEventsToMantis() {
        dataGenerator
                .generateEvents()
                .map((requestEvent) -> new Event(requestEvent.toMap()))
                .flatMap((event) -> Observable.from(publisher.publish(event)
                        .toCompletableFuture()))
                .toBlocking()
                .subscribe((status) -> {
                    log.info("Mantis publish JavaApp send event status => {}", status);
                });
    }

}
