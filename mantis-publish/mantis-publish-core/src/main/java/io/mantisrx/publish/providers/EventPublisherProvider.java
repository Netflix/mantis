/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.publish.providers;

import io.mantisrx.publish.MantisEventPublisher;
import io.mantisrx.publish.StreamManager;
import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.config.MrePublishConfiguration;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Provides an instance of a Mantis {@link EventPublisher}.
 *
 * This class can be created standalone via its constructor or created with any injector implementation.
 */
@Singleton
public class EventPublisherProvider implements Provider<EventPublisher> {

    private final MrePublishConfiguration config;

    private final StreamManager streamManager;

    @Inject
    public EventPublisherProvider(MrePublishConfiguration config, StreamManager streamManager) {
        this.config = config;
        this.streamManager = streamManager;
    }

    @Override
    @Singleton
    public EventPublisher get() {
        return new MantisEventPublisher(config, streamManager);
    }
}
