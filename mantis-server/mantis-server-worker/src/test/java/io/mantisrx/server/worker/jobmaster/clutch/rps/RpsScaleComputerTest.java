package io.mantisrx.server.worker.jobmaster.clutch.rps;

import com.netflix.control.clutch.ClutchConfiguration;
import io.vavr.Tuple;
import io.vavr.control.Option;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RpsScaleComputerTest {
    @Test
    public void testApply() {
        ClutchRpsPIDConfig rpsConfig = new ClutchRpsPIDConfig(0.0, Tuple.of(0.0, 0.0), 0.0, 0.0, Option.of(0.4), Option.of(0.6), Option.of(2.0), Option.of(0.5));
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
