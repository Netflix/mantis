/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.agent.config;

import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.runtime.loader.config.WorkerConfigurationUtils;
import java.util.Properties;

public class StaticPropertiesConfigurationFactory implements ConfigurationFactory {

    private final WorkerConfiguration config;

    public StaticPropertiesConfigurationFactory(Properties props) {
        config = WorkerConfigurationUtils.frmProperties(props, WorkerConfiguration.class);
    }

    @Override
    public WorkerConfiguration getConfig() {
        return this.config;
    }

    @Override
    public String toString() {
        return "StaticPropertiesConfigurationFactory{" +
            ", config=" + config +
            '}';
    }
}
