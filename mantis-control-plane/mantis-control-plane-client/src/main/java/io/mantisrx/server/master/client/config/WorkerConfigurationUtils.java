/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.server.master.client.config;

import io.mantisrx.server.core.MetricsCoercer;
import java.util.Properties;
import org.skife.config.ConfigurationObjectFactory;

public class WorkerConfigurationUtils {
    public static <T extends WorkerConfiguration> T frmProperties(Properties properties, Class<T> tClass) {
        ConfigurationObjectFactory configurationObjectFactory = new ConfigurationObjectFactory(
            properties);
        configurationObjectFactory.addCoercible(new MetricsCoercer(properties));
        configurationObjectFactory.addCoercible(new PluginCoercible<>(MetricsCollector.class, properties));
        return configurationObjectFactory.build(tClass);
    }
}
