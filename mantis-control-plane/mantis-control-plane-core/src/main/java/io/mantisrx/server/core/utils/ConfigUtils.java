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
import java.lang.reflect.Method;
import org.skife.config.Config;

public class ConfigUtils {
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

    private static String retrievePropertyKey(Method method) {
        Config configAnnotation = method.getAnnotation(Config.class);
        String[] values = configAnnotation.value();
        if (values.length != 1) {
            throw new RuntimeException("invalid annotation on: " + method.getName());
        }
        String propertyKey = values[0];
        return propertyKey;
    }
}
