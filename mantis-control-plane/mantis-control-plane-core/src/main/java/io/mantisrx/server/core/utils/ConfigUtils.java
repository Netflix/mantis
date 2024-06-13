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

package io.mantisrx.server.core.utils;

import io.mantisrx.common.properties.MantisPropertiesLoader;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.config.dynamic.StringDynamicProperty;
import io.mantisrx.server.core.CoreConfiguration;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;
import org.skife.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static StringDynamicProperty getDynamicPropertyString(
        Method method,
        String defaultValue,
        MantisPropertiesLoader loader) {
        String propertyKey = retrievePropertyKey(method);
        return new StringDynamicProperty(loader, propertyKey, defaultValue);
    }

    public static StringDynamicProperty getDynamicPropertyString(
        String name,
        Class<? extends CoreConfiguration> clazz,
        String defaultValue,
        MantisPropertiesLoader loader) {
        return getDynamicPropertyString(getMethodFromName(name, clazz), defaultValue, loader);
    }

    private static LongDynamicProperty getDynamicPropertyLong(
        Method method,
        long defaultValue,
        MantisPropertiesLoader loader) {
        String propertyKey = retrievePropertyKey(method);
        return new LongDynamicProperty(loader, propertyKey, defaultValue);
    }

    public static <T> T createInstance(String fullyQualifiedClassName, Class<T> interfaceType) {
        try {
            // Load the class by its name
            Class<?> clazz = Class.forName(fullyQualifiedClassName);
            // Check if the class actually implements the desired interface
            if (!interfaceType.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(fullyQualifiedClassName + " does not implement " + interfaceType.getName());
            }
            // Find the no-args constructor of the class
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            // Create a new instance using the no-args constructor
            Object instance = constructor.newInstance();
            return interfaceType.cast(instance);
        } catch (Exception e) {
            // Handle any exceptions (ClassNotFoundException, NoSuchMethodException, etc.)
            final String msg = "failed to create instance of " + fullyQualifiedClassName;
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public static LongDynamicProperty getDynamicPropertyLong(
        String name,
        Class<? extends CoreConfiguration> clazz,
        long defaultValue,
        MantisPropertiesLoader loader) {
        return getDynamicPropertyLong(getMethodFromName(name, clazz), defaultValue, loader);
    }

    public static Method getMethodFromName(String name, Class<? extends CoreConfiguration> clazz) {
        try {
            return clazz.getMethod(name);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("invalid name: " + name + " for " + clazz.getSimpleName());
        }
    }

    /**
     * This function tries to find a properties file and throws a runtime exception if it cannot.
     * If found any environment variables matching the property key converted to environment variable
     * standard form of capital letters and underscores will take precedence.
     *
     * @param propFile a property file to find
     * @return Properties with overrides from the environment variables
     * @throws RuntimeException if the file can't be found
     */
    public static Properties loadProperties(String propFile) {
        Properties props = new Properties();
        try (InputStream in = findResourceAsStream(propFile)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Can't load properties from the given property file %s: %s", propFile, e.getMessage()), e);
        }

        for (String key : props.stringPropertyNames()) {
            String envVarKey = key.toUpperCase().replace('.', '_');
            String envValue = System.getenv(envVarKey);
            if (envValue != null) {
                props.setProperty(key, envValue);
                logger.info("Override config from env {}: {}.", key, envValue);
            }

        }
        return props;
    }

    private static String retrievePropertyKey(Method method) {
        Config configAnnotation = method.getAnnotation(Config.class);
        String[] values = configAnnotation.value();
        if (values.length != 1) {
            throw new RuntimeException("invalid annotation on: " + method.getName());
        }
        String propertyKey = values[0];
        return propertyKey;
    }



    /**
     * Finds the given resource and returns its input stream. This method seeks the file first from the current working directory,
     * and then in the class path.
     *
     * @param resourceName the name of the resource. It can either be a file name, or a path.
     *
     * @return An {@link java.io.InputStream} instance that represents the found resource. Null otherwise.
     *
     * @throws FileNotFoundException
     */
    private static InputStream findResourceAsStream(String resourceName) throws FileNotFoundException {
        File resource = new File(resourceName);
        if (resource.exists()) {
            return new FileInputStream(resource);
        }

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (is == null) {
            throw new FileNotFoundException(String.format("Can't find property file %s. Make sure the property file is either in your path or in your classpath ", resourceName));
        }

        return is;
    }
}
