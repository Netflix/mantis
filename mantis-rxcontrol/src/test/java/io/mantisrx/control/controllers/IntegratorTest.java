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

import org.junit.Test;

public class IntegratorTest {
    @Test
    public void shouldIntegrateInputs() {
        Integrator integrator = new Integrator(10, -100, 100, 1.0);
        double output = integrator.processStep(10.0);
        assertEquals(20.0, output, 1e-10);

        output = integrator.processStep(20.0);
        assertEquals(40.0, output, 1e-10);

        integrator.setSum(-10.0);
        output = integrator.processStep(-10.0);
        assertEquals(-20.0, output, 1e-10);
    }

    @Test
    public void shouldSupportDecay() {
        Integrator integrator = new Integrator(10, -100, 100, 0.9);
        double output = integrator.processStep(10.0);
        assertEquals(20.0, output, 1e-10);

        output = integrator.processStep(20.0);
        assertEquals(38.0, output, 1e-10);

        output = integrator.processStep(30.0);
        assertEquals(64.2, output, 1e-10);
    }

    @Test
    public void shouldSupportMinMax() {
        Integrator integrator = new Integrator(10, -100, 100, 1.0);
        double output = integrator.processStep(200.0);
        assertEquals(100.0, output, 1e-10);

        output = integrator.processStep(-400.0);
        assertEquals(-100.0, output, 1e-10);
    }
}
