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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.jupiter.api.Test;

public class WorkerPortsTest {

    private final JsonSerializer serializer = new JsonSerializer();
    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)} which expects
     * at least 5 ports: metrics, debug, console, custom.
     */
    @Test
    public void shouldNotConstructWorkerPorts() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Not enough ports.
            new WorkerPorts(Arrays.asList(1, 1, 1, 1));
        });
    }

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)} which cannot construct
     * a WorkerPorts object, because a worker needs a sink to be useful.
     * Otherwise, other workers can't connect to it.
     */
    @Test
    public void shouldNotConstructWorkerPortsWithDuplicatePorts() {
        assertThrows(IllegalStateException.class, () -> {
            // Enough ports, but has duplicate ports.
            new WorkerPorts(Arrays.asList(1, 1, 1, 1, 1));
        });
    }

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)} but was given a port
     * out of range.
     */
    @Test
    public void shouldNotConstructWorkerPortsWithInvalidPortRange() {
        assertThrows(IllegalStateException.class, () -> {
            // Enough ports, but given an invalid port range
            new WorkerPorts(Arrays.asList(1, 1, 1, 1, 65536));
        });
    }

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)}.
     */
    @Test
    public void shouldConstructValidWorkerPorts() {
        WorkerPorts workerPorts = new WorkerPorts(Arrays.asList(1, 2, 3, 4, 5));
    }

    @Test
    public void testIfWorkerPortsIsSerializableByJson() throws Exception {
        final WorkerPorts workerPorts =
                new WorkerPorts(1, 2, 3, 4, 5);
        String workerPortsJson = serializer.toJson(workerPorts);
        assertEquals(workerPortsJson, "{\"metricsPort\":1,\"debugPort\":2,\"consolePort\":3,\"customPort\":4,\"ports\":[5],\"sinkPort\":5}");

        final WorkerPorts actual = serializer.fromJSON(workerPortsJson, WorkerPorts.class);
        assertEquals(workerPorts, actual);
    }

    @Test
    public void testWorkerPortsIsSerializableByJava() {
        final WorkerPorts workerPorts =
                new WorkerPorts(1, 2, 3, 4, 5);
        byte[] serialized = SerializationUtils.serialize(workerPorts);
        assertEquals(workerPorts, SerializationUtils.deserialize(serialized));
    }
}
