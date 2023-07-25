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

import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.common.properties.MantisPropertiesLoader;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServiceRegistry {

    private static Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);
    private final AtomicReference<State> registryRef = new AtomicReference<>(null);
    public static ServiceRegistry INSTANCE = new ServiceRegistry();

    private ServiceRegistry() {
    }

    public void setMantisPropertiesService(MantisPropertiesLoader service) {
        logger.debug("Setting Mantis Properties Service to {}", service);
        if (!registryRef.compareAndSet(null, new State(service, new Exception()))) {
            logger.error(
                "MantisPropertiesService already set to {} as part of the below stacktrace",
                registryRef.get().getPropertiesLoader(), registryRef.get().getStackTrace());
        }
    }


    public MantisPropertiesLoader getPropertiesService() {
        if (registryRef.get() == null) {
            setMantisPropertiesService(loadMantisPropertiesLoader());
        }

        return registryRef.get().getPropertiesLoader();
    }

    private static MantisPropertiesLoader loadMantisPropertiesLoader() {
        MantisPropertiesLoader mpl = new DefaultMantisPropertiesLoader(new Properties());
        try {
            mpl = (MantisPropertiesLoader) Class.forName("com.netflix.mantis.common.properties.MantisFastPropertiesLoader").getConstructor(Properties.class)
                    .newInstance(new Properties());
        } catch (Exception e) {
            logger.warn("Could not load MantisFastPropertiesLoader");
        }
        return mpl;
    }

    @Value
    class State {
        MantisPropertiesLoader propertiesLoader;
        Exception stackTrace;
    }
}
