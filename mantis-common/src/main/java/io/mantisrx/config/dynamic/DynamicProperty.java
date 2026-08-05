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

/**
 * A property that re-reads itself from the {@link MantisPropertiesLoader} roughly once per refresh
 * interval ({@code mantis.config.dynamic.refreshSecs}, default 30s), lazily on whichever thread
 * calls {@link #getValue()}.
 *
 * <p>Safe to read from many threads, which matters because callers sit on request paths (see
 * {@code GlobalApiRequestRateLimiter}): the cached state is published through volatile fields, so a
 * reader never sees a stale or partially-visible value and the steady-state read is lock-free.
 *
 * <p>The refresh itself is deliberately unsynchronized, and volatile alone is enough because the
 * refresh is a blind overwrite: the new value is derived from the loader, never from
 * {@link #lastValue}. So racing refreshers each compute a correct value independently and
 * last-writer-wins leaves a correct one — there is no lost update the way there would be for a
 * read-modify-write such as a counter. Nor do {@link #lastValue} and {@link #lastRefreshTime} need to
 * be updated atomically with respect to each other: the stamp is written before the loader call, so a
 * reader can briefly see a fresh stamp with the previous value, which is just the one-interval
 * staleness the contract already allows. The only cost of a race is a duplicated lookup inside a
 * window the width of one {@link System#getProperty}.
 *
 * <p>That reasoning breaks if a refresh ever becomes dependent on the previous value — rate
 * smoothing, "reject a value more than 2x the last one", parse-failure backoff. Any of those makes
 * this a read-modify-write and would need a lock or a CAS on the refresh stamp.
 */
@Slf4j
public abstract class DynamicProperty<T>  {
    public static final String DYNAMIC_PROPERTY_REFRESH_SECONDS_KEY = "mantis.config.dynamic.refreshSecs";
    protected final MantisPropertiesLoader propertiesLoader;
    protected final String propertyName;
    protected final T defaultValue;
    protected volatile T lastValue;
    protected volatile Instant lastRefreshTime;
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
