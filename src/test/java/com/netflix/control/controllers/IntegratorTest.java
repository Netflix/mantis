package com.netflix.control.controllers;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
}
