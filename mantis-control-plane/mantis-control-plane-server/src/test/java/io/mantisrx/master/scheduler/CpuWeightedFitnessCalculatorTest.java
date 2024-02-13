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

package io.mantisrx.master.scheduler;

import static org.junit.Assert.assertEquals;

import io.mantisrx.runtime.MachineDefinition;
import org.junit.Before;
import org.junit.Test;

public class CpuWeightedFitnessCalculatorTest {

    private CpuWeightedFitnessCalculator calculator;

    @Before
    public void setUp() {
        calculator = new CpuWeightedFitnessCalculator();
    }

    @Test
    public void testCalculate_whenCpuCannotFit() {
        MachineDefinition requested = new MachineDefinition(16, 10000, 2048, 1);
        MachineDefinition available = new MachineDefinition(8, 16000, 2048, 1);  // Less cpu than requested
        assertEquals(0.0, calculator.calculate(requested, available), 0.01);
    }

    @Test
    public void testCalculate_whenMemoryCannotFit() {
        MachineDefinition requested = new MachineDefinition(6, 32000, 2048, 1);
        MachineDefinition available = new MachineDefinition(8, 16000, 2048, 1); // Less memory than requested
        assertEquals(0.0, calculator.calculate(requested, available), 0.01);
    }

    @Test
    public void testCalculate_whenDiskCannotFit() {
        MachineDefinition requested = new MachineDefinition(8, 10000, 2048, 1);
        MachineDefinition available = new MachineDefinition(8, 16000, 1024, 1); // Less disk than requested
        assertEquals(0.0, calculator.calculate(requested, available), 0.01);
    }

    @Test
    public void testCalculate_whenFitPerfectly() {
        MachineDefinition requested = new MachineDefinition(8, 16000, 1024, 1); // Equal to available - disk, net and ports are excluded
        MachineDefinition available = new MachineDefinition(8, 16000, 2048, 3);
        assertEquals(1.0, calculator.calculate(requested, available), 0.01);
    }

    @Test
    public void testCalculate_whenCpuFitExactlyButMemoryIsHigher() {
        MachineDefinition requested = new MachineDefinition(8, 16000, 2048, 1); // cpuCores fit exactly, memory is less than available
        MachineDefinition available = new MachineDefinition(8, 32000, 2048, 1);
        double expected = (18 + 0.5) / 19;
        assertEquals(expected, calculator.calculate(requested, available), 0.01);
    }

    @Test
    public void testCalculate_whenMemoryFitExactlyButCpuIsHigher() {
        MachineDefinition requested = new MachineDefinition(4, 32000, 2048, 1);  // memory fit exactly, cpuCores is less than available
        MachineDefinition available = new MachineDefinition(8, 32000, 2048, 1);
        double expected = ((18*0.5) + 1) / 19;
        assertEquals(expected, calculator.calculate(requested, available), 0.01);
    }
}
