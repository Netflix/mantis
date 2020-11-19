package com.netflix.control.clutch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
