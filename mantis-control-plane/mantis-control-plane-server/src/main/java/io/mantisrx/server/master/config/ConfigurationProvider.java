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

package io.mantisrx.server.master.config;

/**
 * Provides static and global access to configuration objects. The method {@link ConfigurationProvider#initialize(ConfigurationFactory)}
 * must be called before this class can be used.
 *
 * @see io.mantisrx.server.master.config.ConfigurationFactory
 */
public class ConfigurationProvider {

    private static ConfigurationFactory factory;

    public static void initialize(ConfigurationFactory aFactory) {
        factory = aFactory;
    }

    // For testing only
    static ConfigurationFactory reset() {
        ConfigurationFactory current = factory;
        factory = null;

        return current;
    }

    /**
     * @return a {@link io.mantisrx.server.master.config.MasterConfiguration} object.
     *
     * @throws IllegalStateException if the method {@link ConfigurationProvider#initialize(ConfigurationFactory)} is not
     *                               called yet.
     */
    public static MasterConfiguration getConfig() {
        if (factory == null) {
            throw new IllegalStateException(String.format("%s#initialize() must be called first. ", ConfigurationFactory.class.getName()));
        }

        return factory.getConfig();
    }
}
