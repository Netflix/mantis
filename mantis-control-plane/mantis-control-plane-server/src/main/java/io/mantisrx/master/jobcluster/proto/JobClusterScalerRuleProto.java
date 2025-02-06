package io.mantisrx.master.jobcluster.proto;

import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

public class JobClusterScalerRuleProto {

    @EqualsAndHashCode(callSuper = true)
    @Builder
    @Value
    public static class CreateScalerRuleRequest extends BaseRequest {
        String jobClusterName;
        ScalerConfig scalerConfig;
        TriggerConfig triggerConfig;
        Map<String, String> metadata;
    }

    @Builder
    @Value
    public static class ScalerConfig {
        String type; // only support standard scaling policy for now
        StageScalingPolicy scalingPolicy;

        /**
         * Desired size when this config is triggered.
         */
        int desireSize;
    }

    @Builder
    @Value
    public static class TriggerConfig {
        String triggerType;
        String scheduleCron;
        String scheduleDuration;
        String customTrigger;
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class DeleteScalerRuleRequest extends BaseRequest {
        String jobClusterName;
        String ruleId;

        public DeleteScalerRuleRequest(String jobClusterName, String ruleId) {
            super();
            Preconditions.checkNotNull(jobClusterName, "jobClusterName cannot be null");
            Preconditions.checkNotNull(ruleId, "ruleId cannot be null");

            this.jobClusterName = jobClusterName;
            this.ruleId = ruleId;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class GetScalerRulesRequest extends BaseRequest {
        String jobClusterName;

        public GetScalerRulesRequest(String jobClusterName) {
            super();
            Preconditions.checkNotNull(jobClusterName, "jobClusterName cannot be null");
            this.jobClusterName = jobClusterName;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @SuperBuilder
    @Value
    public static class CreateScalerRuleResponse extends BaseResponse {
        String ruleId;
    }

    @EqualsAndHashCode(callSuper = true)
    @SuperBuilder
    @Value
    public static class GetScalerRulesResponse extends BaseResponse {
        List<ScalerRule> rules;
    }

    @Builder
    @Value
    public static class ScalerRule {
        String ruleId;
        ScalerConfig scalerConfig;
        TriggerConfig triggerConfig;
        Map<String, String> metadata;
    }

    @EqualsAndHashCode(callSuper = true)
    @SuperBuilder
    @Value
    public static class DeleteScalerRuleResponse extends BaseResponse  {
    }
}
