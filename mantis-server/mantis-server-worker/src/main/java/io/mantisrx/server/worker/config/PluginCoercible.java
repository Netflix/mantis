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

package io.mantisrx.server.worker.config;

import io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import org.apache.flink.util.ExceptionUtils;
import org.skife.config.Coercer;
import org.skife.config.Coercible;

@RequiredArgsConstructor
public class PluginCoercible<T> implements Coercible<T> {

    private final Class<T> tClass;
    private final Properties properties;

    @SuppressWarnings("unchecked")
    public Coercer<T> accept(final Class<?> type) {
        if (tClass.isAssignableFrom(type)) {
            return value -> {
                try {
                    Class<?> derivedType = Class.forName(value);
                    Preconditions.checkArgument(derivedType.isAssignableFrom(tClass));
                    Method candidate = derivedType.getMethod("valueOf", Properties.class);
                    // Method must be 'static valueOf(Properties)' and return the type in question.
                    Preconditions.checkArgument(Modifier.isStatic(candidate.getModifiers()));
                    Preconditions.checkArgument(candidate.getReturnType().isAssignableFrom(type));

                    return (T) candidate.invoke(null, properties);
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                    return null;
                }
            };
        } else {
            return null;
        }
    }
}
