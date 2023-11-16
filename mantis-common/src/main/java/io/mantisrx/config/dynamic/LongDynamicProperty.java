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

package io.mantisrx.config.dynamic;

import io.mantisrx.common.properties.MantisPropertiesLoader;
import java.time.Clock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LongDynamicProperty extends DynamicProperty<Long> {
    public LongDynamicProperty(MantisPropertiesLoader propertiesLoader, String propertyName, Long defaultValue) {
        super(propertiesLoader, propertyName, defaultValue);
    }

    public LongDynamicProperty(MantisPropertiesLoader propertiesLoader, String propertyName, Long defaultValue,
        Clock clock) {
        super(propertiesLoader, propertyName, defaultValue, clock);
    }

    @Override
    protected Long convertFromString(String newStrVal) {
        try {
            return Long.parseLong(newStrVal);
        } catch (NumberFormatException e) {
            throw new RuntimeException("invalid long value: " + newStrVal);
        }
    }
}
