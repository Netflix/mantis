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

package io.mantisrx.control.clutch;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ClutchConfigurationTest {
    @Test
    public void shouldCreateClutchConfiguration() {
        ClutchConfiguration config = ClutchConfiguration.builder().kd(1.0).build();
        assertEquals(1.0, config.kd, 1e-10);
        assertEquals(1.0, config.integralDecay, 1e-10);

        config = ClutchConfiguration.builder().kd(1.0).integralDecay(0.9).build();
        assertEquals(1.0, config.kd, 1e-10);
        assertEquals(0.9, config.integralDecay, 1e-10);
    }
}
