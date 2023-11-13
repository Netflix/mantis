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

package io.mantisrx.common.properties;

import java.time.Clock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringDynamicProperty extends DynamicProperty<String> {

    public StringDynamicProperty(MantisPropertiesLoader propertiesLoader, String propertyName, String defaultValue) {
        super(propertiesLoader, propertyName, defaultValue);
    }

    public StringDynamicProperty(MantisPropertiesLoader propertiesLoader, String propertyName, String defaultValue,
        Clock clock) {
        super(propertiesLoader, propertyName, defaultValue, clock);
    }
    @Override
    public String getValue() {
        if (shouldRefresh()) {
            String newVal = this.getStringValue();
            if (this.lastValue != newVal) {
                log.info("[DP: {}] value changed from {} to {}", this.propertyName, this.lastValue, newVal);
            }
            this.lastValue = newVal;
        }

        return this.lastValue;
    }
}
