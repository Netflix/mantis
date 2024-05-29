/*
 * Copyright 2024 Netflix, Inc.
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

import java.lang.reflect.InvocationTargetException;
import org.skife.config.Coercer;
import org.skife.config.Coercible;

public class LeaderMonitorFactoryCoercer implements Coercible<ILeaderMonitorFactory> {
    @Override
    public Coercer<ILeaderMonitorFactory> accept(Class<?> clazz) {
        if(ILeaderMonitorFactory.class.isAssignableFrom(clazz)) {
            return new Coercer<ILeaderMonitorFactory>() {
                @Override
                public ILeaderMonitorFactory coerce(String className) {
                    try {
                        return (ILeaderMonitorFactory) Class.forName(className).getDeclaredConstructor()
                            .newInstance();
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                             NoSuchMethodException | ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
        return null;
    }
}
