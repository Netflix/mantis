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

package io.mantisrx.control.controllers;

import static org.junit.Assert.assertEquals;

import io.mantisrx.shaded.com.google.common.util.concurrent.AtomicDouble;
import org.junit.Test;

public class PIDControllerTest {
    @Test
    public void shouldComputeSignal() {
        PIDController controller = new PIDController(1.0, 1.0, 1.0, 1.0, new AtomicDouble(1.0), 0.9);
        double signal = controller.processStep(10.0);
        assertEquals(30, signal, 1e-10);

        signal = controller.processStep(10.0);
        // p: 10, i: 19, d: 0
        assertEquals(29, signal, 1e-10);

        signal = controller.processStep(20.0);
        // p: 20, i: 27.1, d: 10
        assertEquals(67.1, signal, 1e-10);
    }
}
