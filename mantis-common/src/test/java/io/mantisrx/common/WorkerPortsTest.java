/*
 * Copyright 2021 Netflix, Inc.
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
package io.mantisrx.common;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;


public class WorkerPortsTest {

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)} which expects
     * at least 5 ports: metrics, debug, console, custom.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWorkerPorts() {
        // Not enough ports.
        new WorkerPorts(Arrays.asList(1, 1, 1, 1));
    }

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)} which cannot construct
     * a WorkerPorts object, because a worker needs a sink to be useful.
     * Otherwise, other workers can't connect to it.
     */
    @Test(expected = IllegalStateException.class)
    public void shouldNotConstructWorkerPortsWithDuplicatePorts() {
        // Enough ports, but has duplicate ports.
        new WorkerPorts(Arrays.asList(1, 1, 1, 1, 1));
    }

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)} but was given a port
     * out of range.
     */
    @Test(expected = IllegalStateException.class)
    public void shouldNotConstructWorkerPortsWithInvalidPortRange() {
        // Enough ports, but given an invalid port range
        new WorkerPorts(Arrays.asList(1, 1, 1, 1, 65536));
    }

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)}.
     */
    @Test
    public void shouldConstructValidWorkerPorts() {
        WorkerPorts workerPorts = new WorkerPorts(Arrays.asList(1, 2, 3, 4, 5));
        assertTrue(workerPorts.isValid());
    }
}