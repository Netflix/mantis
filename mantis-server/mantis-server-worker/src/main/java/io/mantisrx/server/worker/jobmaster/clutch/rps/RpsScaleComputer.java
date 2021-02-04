package io.mantisrx.server.worker.jobmaster.clutch.rps;

import com.netflix.control.clutch.ClutchConfiguration;
import com.netflix.control.clutch.IScaleComputer;

public class RpsScaleComputer implements IScaleComputer {
    private final ClutchRpsPIDConfig rpsConfig;

    public RpsScaleComputer(ClutchRpsPIDConfig rpsConfig) {
        if (rpsConfig == null) {
            rpsConfig = ClutchRpsPIDConfig.DEFAULT;
        }
        this.rpsConfig = rpsConfig;
    }
    public Double apply(ClutchConfiguration config, Long currentScale, Double delta) {
        double scaleUpPct = rpsConfig.getScaleUpAbovePct() / 100.0;
        double scaleDownPct = rpsConfig.getScaleDownBelowPct() / 100.0;
        if (delta > -scaleDownPct && delta < scaleUpPct) {
            return (double) currentScale;
        }
        if (delta >= scaleUpPct) {
            delta = delta * rpsConfig.getScaleUpMultiplier();
        }
        if (delta <= -scaleDownPct) {
            delta = delta * rpsConfig.getScaleDownMultiplier();
        }

        // delta is a percentage, actual increase/decrease is computed as percentage of current scale.
        double scale = Math.round(currentScale + currentScale * delta);

        scale = Math.min(config.getMaxSize(), Math.max(config.getMinSize(), scale));
        return scale;
    }
}
