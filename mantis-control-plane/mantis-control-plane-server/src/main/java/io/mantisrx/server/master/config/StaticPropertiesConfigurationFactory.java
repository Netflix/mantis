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

import io.mantisrx.server.core.MetricsCoercer;
import java.util.Properties;
import org.skife.config.ConfigurationObjectFactory;


public class StaticPropertiesConfigurationFactory implements ConfigurationFactory {

    private final ConfigurationObjectFactory delegate;
    private final MasterConfiguration config;
    private final Properties properties;

    public StaticPropertiesConfigurationFactory(Properties props) {
        this.properties = props;
        delegate = new ConfigurationObjectFactory(props);
        delegate.addCoercible(new MetricsCoercer(props));
        delegate.addCoercible(clazz -> {
            return className -> {
                try {
                    if (clazz.isAssignableFrom(Class.forName(className))) {
                        try {
                            return Class.forName(className).newInstance();
                        } catch (Exception e) {
                            throw new IllegalArgumentException(
                                String.format(
                                    "The value %s is not a valid class name for %s implementation. ",
                                    className,
                                    clazz.getName()));
                        }
                    } else {
                        return null;
                    }
                } catch (ClassNotFoundException e) {
                    return null;
                }
            };
        });

        config = delegate.build(MasterConfiguration.class);

    }

    @Override
    public MasterConfiguration getConfig() {
        return this.config;
    }

    @Override
    public String toString() {
        return "StaticPropertiesConfigurationFactory{" +
                "delegate=" + delegate +
                ", config=" + config +
                '}';
    }
}
