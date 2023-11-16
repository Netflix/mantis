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
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DynamicProperty<T>  {
    public static final String DYNAMIC_PROPERTY_REFRESH_SECONDS_KEY = "mantis.config.dynamic.refreshSecs";
    protected final MantisPropertiesLoader propertiesLoader;
    protected final String propertyName;
    protected final T defaultValue;
    protected T lastValue;
    protected Instant lastRefreshTime;
    private final Duration refreshDuration;
    private final Clock clock;

    public DynamicProperty(MantisPropertiesLoader propertiesLoader, String propertyName, T defaultValue, Clock clock) {
        this.propertiesLoader = propertiesLoader;
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.lastValue = defaultValue;
        this.clock = clock;
        this.lastRefreshTime = Instant.MIN;

        try
        {
            this.refreshDuration = Duration.ofSeconds(Long.parseLong(
                propertiesLoader.getStringValue(DYNAMIC_PROPERTY_REFRESH_SECONDS_KEY, "30")));
        } catch (NumberFormatException ex) {
            throw new RuntimeException("invalid refresh secs for dynamic property: " + propertyName);
        }
    }

    public DynamicProperty(MantisPropertiesLoader propertiesLoader, String propertyName, T defaultValue) {
        this(propertiesLoader, propertyName, defaultValue, Clock.systemDefaultZone());
    }

    protected String getStringValue() {
        this.lastRefreshTime = this.clock.instant();
        return this.propertiesLoader.getStringValue(this.propertyName, this.lastValue.toString());
    }

    private boolean shouldRefresh() {
        return this.clock.instant().isAfter(this.lastRefreshTime.plus(this.refreshDuration));
    }

    protected abstract T convertFromString(String newStrVal);

    public T getValue() {
        if (shouldRefresh()) {
            String newStrVal = this.getStringValue();
            T newVal = convertFromString(newStrVal);
            if (!Objects.equals(this.lastValue, newVal)) {
                log.info("[DP: {}] value changed from {} to {}", this.propertyName, this.lastValue, newVal);
            }
            this.lastValue = newVal;
        }

        return this.lastValue;
    }
}
