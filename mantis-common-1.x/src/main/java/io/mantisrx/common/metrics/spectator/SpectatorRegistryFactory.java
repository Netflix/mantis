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

package io.mantisrx.common.metrics.spectator;

import java.util.concurrent.atomic.AtomicReference;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpectatorRegistryFactory {

    private static final Logger logger = LoggerFactory.getLogger(SpectatorRegistryFactory.class);

    private static final AtomicReference<Registry> registryRef = new AtomicReference<>(null);

    public static Registry getRegistry() {
        if (registryRef.get() == null) {
            return Spectator.globalRegistry();
        } else {
            return registryRef.get();
        }
    }

    public static void setRegistry(final Registry registry) {
        if (registry != null && registryRef.compareAndSet(null, registry)) {
            logger.info("spectator registry : {}", registryRef.get().getClass().getCanonicalName());
        }
    }
}