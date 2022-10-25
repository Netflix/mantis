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

package io.mantisrx.server.worker.config;


import io.mantisrx.runtime.loader.config.WorkerConfiguration;

/**
 * Provides static and global access to configuration objects. The method io.mantisrx.server.master.config.ConfigurationProvider#initialize(ConfigurationFactory)
 * must be called before this class can be used.
 * <p>
 * see io.mantisrx.server.worker.config.ConfigurationFactory
 */
public class ConfigurationProvider {

    private static ConfigurationFactory factory;

    public static void initialize(ConfigurationFactory aFactory) {
        factory = aFactory;
    }

    /**
     * @return a io.mantisrx.server.master.config.MasterConfiguration object.
     *
     * @throws java.lang.IllegalStateException if the method io.mantisrx.server.master.config.ConfigurationProvider#initialize(ConfigurationFactory) is not
     *                                         called yet.
     */
    public static WorkerConfiguration getConfig() {
        if (factory == null) {
            throw new IllegalStateException(String.format("%s#initialize() must be called first. ", ConfigurationFactory.class.getName()));
        }

        return factory.getConfig();
    }
}
