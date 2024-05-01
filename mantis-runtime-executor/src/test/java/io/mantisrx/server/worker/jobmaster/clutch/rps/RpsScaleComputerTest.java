/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.server.worker.jobmaster.clutch.rps;

import static org.junit.Assert.assertEquals;

import io.mantisrx.control.clutch.ClutchConfiguration;
import io.vavr.Tuple;
import io.vavr.control.Option;
import org.junit.Test;

public class RpsScaleComputerTest {
    @Test
    public void testApply() {
        ClutchRpsPIDConfig rpsConfig = new ClutchRpsPIDConfig(0.0, Tuple.of(0.0, 0.0), 0.0, 0.0, Option.none(), Option.of(40.0), Option.of(60.0), Option.of(2.0), Option.of(0.5));
        RpsScaleComputer scaleComputer = new RpsScaleComputer(rpsConfig);
        ClutchConfiguration config = ClutchConfiguration.builder().minSize(1).maxSize(1000).build();

        double scale = scaleComputer.apply(config, 100L, 0.1);
        assertEquals(100, scale, 1e-10);

        scale = scaleComputer.apply(config, 100L, 0.5);
        assertEquals(200, scale, 1e-10);

        scale = scaleComputer.apply(config, 100L, -0.7);
        assertEquals(65, scale, 1e-10);
    }

    @Test
    public void testDefaultConfig() {
        RpsScaleComputer scaleComputer = new RpsScaleComputer(null);
        ClutchConfiguration config = ClutchConfiguration.builder().minSize(1).maxSize(1000).build();

        double scale = scaleComputer.apply(config, 100L, 0.1);
        assertEquals(110, scale, 1e-10);

        scale = scaleComputer.apply(config, 100L, 0.5);
        assertEquals(150, scale, 1e-10);

        scale = scaleComputer.apply(config, 100L, -0.7);
        assertEquals(30, scale, 1e-10);
    }
}
