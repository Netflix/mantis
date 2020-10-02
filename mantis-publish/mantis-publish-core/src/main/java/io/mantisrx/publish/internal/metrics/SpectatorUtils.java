/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.publish.internal.metrics;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.spectator.impl.AtomicDouble;


public class SpectatorUtils {

    public static Counter buildAndRegisterCounter(Registry registry, String name) {
        Id id = registry.createId(name);
        return registry.counter(id);
    }

    public static AtomicDouble buildAndRegisterGauge(Registry registry, String name) {
        Id id = registry.createId(name);
        return PolledMeter.using(registry).withId(id).monitorValue(new AtomicDouble());
    }

    public static Timer buildAndRegisterTimer(Registry registry, String name) {
        Id id = registry.createId(name);
        return registry.timer(id);
    }

    public static Counter buildAndRegisterCounter(Registry registry, String name, String... tags) {
        Id id = registry.createId(name).withTags(tags);
        return registry.counter(id);
    }

    public static AtomicDouble buildAndRegisterGauge(Registry registry, String name, String... tags) {
        Id id = registry.createId(name).withTags(tags);
        return PolledMeter.using(registry).withId(id).monitorValue(new AtomicDouble());

    }

    public static Timer buildAndRegisterTimer(Registry registry, String name, String... tags) {
        Id id = registry.createId(name).withTags(tags);
        return registry.timer(id);
    }
}
