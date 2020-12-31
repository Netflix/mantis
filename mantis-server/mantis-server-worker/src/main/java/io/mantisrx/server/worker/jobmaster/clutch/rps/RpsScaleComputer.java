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
        if (delta > -rpsConfig.getScaleDownBelowPct() && delta < rpsConfig.getScaleUpAbovePct()) {
            return (double) currentScale;
        }
        if (delta >= rpsConfig.getScaleUpAbovePct()) {
            delta = delta * rpsConfig.getScaleUpMultiplier();
        }
        if (delta <= -rpsConfig.getScaleDownBelowPct()) {
            delta = delta * rpsConfig.getScaleDownMultiplier();
        }

        double scale = Math.round(currentScale + currentScale * delta);

        scale = Math.min(config.getMaxSize(), Math.max(config.getMinSize(), scale));
        return scale;
    }
}
