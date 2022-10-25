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
 * An implementation of this class should return an instance of io.mantisrx.server.master.config.MasterConfiguration.
 * We create this factory because it's possible that the logic of creating a io.mantisrx.server.master.config.MasterConfiguration
 * can change depending on the user or environment.
 * <p>
 * see io.mantisrx.server.master.config.ConfigurationProvider
 */
public interface ConfigurationFactory {

    WorkerConfiguration getConfig();
}
