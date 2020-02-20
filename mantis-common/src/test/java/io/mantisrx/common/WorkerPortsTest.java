package io.mantisrx.common;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;


public class WorkerPortsTest {

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)} which expects
     * at least 4 ports: metrics, debug, console, custom.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWorkerPorts() {
        new WorkerPorts(Arrays.asList(1, 1, 1));
    }

    /**
     * Uses legacy constructor {@link WorkerPorts#WorkerPorts(List)} which can construct
     * a WorkerPorts object, but is technically invalid because a worker needs a sink
     * to be useful. Otherwise, other workers can't connect to it.
     */
    @Test
    public void shouldConstructInvalidWorkerPorts() {
        // Not enough ports.
        WorkerPorts workerPorts = new WorkerPorts(Arrays.asList(1, 1, 1, 1));
        assertFalse(workerPorts.isValid());

        // Enough ports, but has duplicate ports.
        workerPorts = new WorkerPorts(Arrays.asList(1, 1, 1, 1, 1));
        assertFalse(workerPorts.isValid());
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