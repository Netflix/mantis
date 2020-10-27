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

package io.mantisrx.server.core;

import java.util.Map.Entry;
import java.util.Properties;

import org.skife.config.Coercer;
import org.skife.config.Coercible;


public class MetricsCoercer implements Coercible<MetricsPublisher> {

    private Properties props;

    public MetricsCoercer(Properties props) {
        this.props = props;
    }

    @Override
    public Coercer<MetricsPublisher> accept(Class<?> clazz) {
        if (MetricsPublisher.class.isAssignableFrom(clazz)) {
            return new Coercer<MetricsPublisher>() {
                @Override
                public MetricsPublisher coerce(String className) {
                    try {
                        // get properties for publisher
                        Properties publishProperties = new Properties();
                        String configPrefix =
                                (String) props.get("mantis.metricsPublisher.config.prefix");
                        if (configPrefix != null && configPrefix.length() > 0) {
                            for (Entry<Object, Object> entry : props.entrySet()) {
                                if (entry.getKey() instanceof String &&
                                        ((String) entry.getKey()).startsWith(configPrefix)) {
                                    publishProperties.put(entry.getKey(), entry.getValue());
                                }
                            }
                        }
                        return (MetricsPublisher) Class.forName(className).getConstructor(Properties.class)
                                .newInstance(publishProperties);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "The value %s is not a valid class name for %s implementation. ",
                                        className,
                                        MetricsPublisher.class.getName()
                                ), e);
                    }
                }
            };
        }
        return null;
    }
}
