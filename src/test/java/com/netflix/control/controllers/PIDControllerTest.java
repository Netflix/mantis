package com.netflix.control.controllers;

import com.google.common.util.concurrent.AtomicDouble;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
