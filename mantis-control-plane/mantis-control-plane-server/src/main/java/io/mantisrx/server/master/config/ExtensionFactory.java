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

package io.mantisrx.server.master.config;

import io.mantisrx.master.jobcluster.job.NoopCostsCalculatorFactory;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * A factory class that creates an object of type T from a class name and a set of properties.
 * <p>
 * The extension class is expected to have a method named createObject that takes a Properties
 * object as an argument. The method is expected to return an object of type T. For example, see
 * {@link NoopCostsCalculatorFactory}.
 * </p>
 */
public class ExtensionFactory {

    @SuppressWarnings({"unchecked"})
    public static <T> T createObject(String className, Properties properties) throws Exception {
        Object extension = Class.forName(className).newInstance();
        // check if extension has a method named createObject
        Method method = extension.getClass().getMethod("createObject", Properties.class);
        Object ret = method.invoke(extension, properties);
        // cast the object to type T
        return (T) ret;
    }
}
