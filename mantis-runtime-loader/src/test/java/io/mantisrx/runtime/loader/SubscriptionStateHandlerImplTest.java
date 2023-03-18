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

package io.mantisrx.runtime.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.mantisrx.common.util.DelegateClock;
import io.mantisrx.runtime.loader.SubscriptionStateHandlerImpl.SubscriptionState;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class SubscriptionStateHandlerImplTest {
    @Test
    public void testSubscriptionState() {
        final AtomicReference<Clock> clock = new AtomicReference<>(Clock.fixed(Instant.ofEpochSecond(1), ZoneId.systemDefault()));
        SubscriptionState state = SubscriptionState.of(new DelegateClock(clock));
        assertFalse(state.isSubscribed());

        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(100)));
        assertTrue(state.isUnsubscribedFor(Duration.ofSeconds(100)));
        assertTrue(state.hasRunFor(Duration.ofSeconds(100)));
        assertFalse(state.hasRunFor(Duration.ofSeconds(101)));
        assertEquals(Duration.ofSeconds(100), state.getUnsubscribedDuration());

        state = state.onSinkSubscribed();
        assertTrue(state.isSubscribed());
        assertFalse(state.isUnsubscribedFor(Duration.ofSeconds(10)));
        assertTrue(state.hasRunFor(Duration.ofSeconds(100)));

        clock.updateAndGet(c -> Clock.offset(c, Duration.ofSeconds(100)));
        state = state.onSinkUnsubscribed();
        assertTrue(state.isUnsubscribedFor(Duration.ofSeconds(0)));
        assertFalse(state.isUnsubscribedFor(Duration.ofSeconds(1)));
        assertTrue(state.hasRunFor(Duration.ofSeconds(200)));
        assertFalse(state.hasRunFor(Duration.ofSeconds(201)));
        assertEquals(Duration.ofSeconds(0), state.getUnsubscribedDuration());
    }

}
