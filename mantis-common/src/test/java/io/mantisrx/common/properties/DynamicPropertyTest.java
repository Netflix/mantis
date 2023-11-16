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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.mantisrx.common.util.DelegateClock;
import io.mantisrx.config.dynamic.DynamicProperty;
import io.mantisrx.config.dynamic.LongDynamicProperty;
import io.mantisrx.config.dynamic.StringDynamicProperty;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class DynamicPropertyTest {
    @Test
    public void testStringDynamicProperty() throws Exception {
        MantisPropertiesLoader mockloader = mock(MantisPropertiesLoader.class);
        final String key1 = "key1";
        final String defaultVal1 = "defaultVal1";
        final String val2 = "val2";
        final String val3 = "val3";
        when(mockloader.getStringValue(eq(DynamicProperty.DYNAMIC_PROPERTY_REFRESH_SECONDS_KEY), ArgumentMatchers.any()))
            .thenReturn("10");
        when(mockloader.getStringValue(key1, defaultVal1)).thenReturn(defaultVal1);

        final AtomicReference<Clock> clock = new AtomicReference<>(
            Clock.fixed(Instant.ofEpochSecond(1), ZoneId.systemDefault()));

        StringDynamicProperty stringFP = new StringDynamicProperty(mockloader, key1, defaultVal1,
            new DelegateClock(clock));
        assertEquals(defaultVal1, stringFP.getValue());

        when(mockloader.getStringValue(key1, defaultVal1)).thenReturn(val2);
        when(mockloader.getStringValue(key1, val2)).thenReturn(val3);

        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(1)));
        assertEquals(defaultVal1, stringFP.getValue());

        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(10)));
        assertEquals(val2, stringFP.getValue());

        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(11)));
        assertEquals(val3, stringFP.getValue());
        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(2)));
        assertEquals(val3, stringFP.getValue());
    }

    @Test
    public void testAckInstance() throws Exception {
        MantisPropertiesLoader mockloader = mock(MantisPropertiesLoader.class);
        final String key1 = "key1";
        final Long defaultVal1 = 1L;
        final Long val2 = 2L;
        final Long val3 = 3L;
        when(mockloader.getStringValue(eq(DynamicProperty.DYNAMIC_PROPERTY_REFRESH_SECONDS_KEY), ArgumentMatchers.any()))
            .thenReturn("10");
        when(mockloader.getStringValue(key1, defaultVal1.toString())).thenReturn(defaultVal1.toString());

        final AtomicReference<Clock> clock = new AtomicReference<>(
            Clock.fixed(Instant.ofEpochSecond(1), ZoneId.systemDefault()));

        LongDynamicProperty longFP = new LongDynamicProperty(mockloader, key1, defaultVal1,
            new DelegateClock(clock));
        assertEquals(defaultVal1, longFP.getValue());

        when(mockloader.getStringValue(key1, defaultVal1.toString())).thenReturn(val2.toString());
        when(mockloader.getStringValue(key1, val2.toString())).thenReturn(val3.toString());

        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(1)));
        assertEquals(defaultVal1, longFP.getValue());

        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(10)));
        assertEquals(val2, longFP.getValue());

        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(11)));
        assertEquals(val3, longFP.getValue());
        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(2)));
        assertEquals(val3, longFP.getValue());
    }
}
