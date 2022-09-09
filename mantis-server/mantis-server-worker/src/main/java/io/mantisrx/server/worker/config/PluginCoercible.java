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

import static org.skife.config.DefaultCoercibles.convertException;

import io.mantisrx.shaded.org.apache.curator.shaded.com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import org.skife.config.Coercer;
import org.skife.config.Coercible;

@RequiredArgsConstructor
public class PluginCoercible<T> implements Coercible<T> {

    private final Class<T> tClass;

    public Coercer<T> accept(final Class<?> type) {

        Coercer<T> coercer = null;
            if (tClass.isAssignableFrom(type)) {
                coercer = new Coercer<T>() {
                    @Override
                    public T coerce(String value) {
                        try {
                            Class<?> derivedType = Class.forName(value);
                            Preconditions.checkArgument(derivedType.isAssignableFrom(tClass));
                            Method candidate = derivedType.getMethod("valueOf", Properties.class);
                            Preconditions.checkArgument(Modifier.isStatic(candidate.getModifiers()));
                            Preconditions.checkArgument(candidate.getReturnType().isAssignableFrom(type));
                        } catch (Exception e) {
                            throw convertException(e);
                        }
                    }
                }
            } else {
                return null;
            }
            // Method must be 'static valueOf(Properties)' and return the type in question.
//            Method candidate = type.getMethod("valueOf", Properties.class);
//            if (!Modifier.isStatic(candidate.getModifiers())) {
//                // not static.
//                candidate = null;
//            }
//            else if (!candidate.getReturnType().isAssignableFrom(type)) {
//                // does not return the right type.
//                candidate = null;
//            }

            if (candidate != null) {
                final Method valueOfMethod = candidate;

                coercer = new Coercer<Object>() {
                    public Object coerce(final String value)
                    {
                        try {
                            return value == null ? null : valueOfMethod.invoke(null, value);
                        }
                        catch (Exception e) {
                            throw convertException(e);
                        }
                    }
                };
            }
        }
        catch(NoSuchMethodException nsme) {
            // Don't do anything, the class does not have a method.
        }

        coercerMap.put(type, coercer);
        return coercer;
    }
}
