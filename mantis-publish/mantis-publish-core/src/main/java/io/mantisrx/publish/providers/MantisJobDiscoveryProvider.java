/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.publish.providers;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.ipc.http.HttpClient;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.internal.discovery.MantisJobDiscoveryCachingImpl;
import io.mantisrx.publish.internal.discovery.mantisapi.DefaultMantisApiClient;

/**
 * Provides an instance of a Mantis {@link MantisJobDiscovery}.
 *
 * This class can be created standalone via its constructor or created with any injector implementation.
 */
@Singleton
public class MantisJobDiscoveryProvider implements Provider<MantisJobDiscovery> {

    private final MrePublishConfiguration configuration;

    private final Registry registry;

    @Inject
    public MantisJobDiscoveryProvider(MrePublishConfiguration configuration, Registry registry) {
        this.configuration = configuration;
        this.registry = registry;
    }

    @Override
    @Singleton
    public MantisJobDiscovery get() {
        return new MantisJobDiscoveryCachingImpl(configuration, registry,
                new DefaultMantisApiClient(configuration, HttpClient.create(registry)));
    }
}
