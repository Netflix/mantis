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

package io.mantisrx.publish;


import javax.inject.Singleton;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.publish.api.Event;


@Singleton
public class NoOpTee implements Tee {
    private final Registry registry;
    private final Counter noOpTeeInvoked;

    public NoOpTee(Registry registry) {
        this.registry = registry;
        this.noOpTeeInvoked = registry.counter("noOpTeeInvoked");
    }

    @Override
    public void tee(String streamName, Event event) {
        // increment a counter for operational visibility,
        // as this usually indicates the Tee was enabled but a more concrete implementation
        // was not bound to the Tee interface during the mantis publish client initialization
        noOpTeeInvoked.increment();
    }
}