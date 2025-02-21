package io.mantisrx.server.worker.jobmaster.rules;

import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestRuleUtils {
    public static JobScalingRule createPerpetualRule(String ruleId, String jobId) {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
            .type("standard")
            .scalingPolicy(createDefaultStageScalingPolicy())
            .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
            .triggerType(JobScalingRule.TRIGGER_TYPE_PERPETUAL)
            .build();

        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(Collections.emptyMap())
            .build();
    }

    public static JobScalingRule createPerpetualRuleWithDesireSize(String ruleId, String jobId) {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
            .type("standard")
            .scalingPolicy(createDefaultStageScalingPolicy())
            .stageDesireSize(Collections.singletonMap(1, 10))
            .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
            .triggerType(JobScalingRule.TRIGGER_TYPE_PERPETUAL)
            .build();

        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(Collections.emptyMap())
            .build();
    }

    public static JobScalingRule createScheduleRule(String ruleId, String cronSpec, String duration) {
        JobScalingRule.ScalerConfig scalerConfig = JobScalingRule.ScalerConfig.builder()
            .type("schedule")
            .scalingPolicy(createDefaultStageScalingPolicy())
            .build();

        JobScalingRule.TriggerConfig triggerConfig = JobScalingRule.TriggerConfig.builder()
            .triggerType(JobScalingRule.TRIGGER_TYPE_SCHEDULE)
            .scheduleCron(cronSpec)
            .scheduleDuration(duration)
            .build();

        return JobScalingRule.builder()
            .ruleId(ruleId)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(Collections.emptyMap())
            .build();
    }

    public static StageScalingPolicy createDefaultStageScalingPolicy(int type) {
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> strategies = new HashMap<>();

        StageScalingPolicy.ScalingReason reason = StageScalingPolicy.ScalingReason.values()[type];
        strategies.put(reason, new StageScalingPolicy.Strategy(
            reason,
            0.3,
            0.7,
            new StageScalingPolicy.RollingCount(3, 5)
        ));

        return new StageScalingPolicy(
            0, // stage
            1, // min
            10, // max
            1, // increment
            1, // decrement
            300, // coolDownSecs
            strategies,
            false // allowAutoScaleManager
        );
    }

    public static StageScalingPolicy createDefaultStageScalingPolicy() {
        return createDefaultStageScalingPolicy(0);
    }
}
