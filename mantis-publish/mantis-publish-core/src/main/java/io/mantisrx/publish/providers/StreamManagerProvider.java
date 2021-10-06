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

import com.netflix.spectator.api.Registry;
import io.mantisrx.publish.StreamManager;
import io.mantisrx.publish.config.MrePublishConfiguration;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Provides an instance of a Mantis {@link StreamManager}.
 *
 * This class can be created standalone via its constructor or created with any injector implementation.
 */
@Singleton
public class StreamManagerProvider implements Provider<StreamManager> {

    private final MrePublishConfiguration configuration;

    private final Registry registry;

    @Inject
    public StreamManagerProvider(MrePublishConfiguration configuration, Registry registry) {
        this.configuration = configuration;
        this.registry = registry;
    }

    @Override
    @Singleton
    public StreamManager get() {
        return new StreamManager(registry, configuration);
    }
}
