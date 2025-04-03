package io.mantisrx.master.jobcluster.proto;

import com.netflix.spectator.impl.Preconditions;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.core.JobScalerRuleInfo;
import io.mantisrx.server.master.domain.JobId;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import rx.Observable;
import rx.subjects.BehaviorSubject;

import java.util.List;
import java.util.Map;

public class JobClusterScalerRuleProto {

    @EqualsAndHashCode(callSuper = true)
    @Builder
    @Value
    public static class CreateScalerRuleRequest extends BaseRequest {
        String jobClusterName;
        JobScalingRule.ScalerConfig scalerConfig;
        JobScalingRule.TriggerConfig triggerConfig;
        Map<String, String> metadata;
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
    @Value
    public static class GetJobScalerRuleStreamRequest extends BaseRequest {
        JobId jobId;

        public GetJobScalerRuleStreamRequest(final JobId jobId) {
            super();
            Preconditions.checkNotNull(jobId, "jobId cannot be null");
            this.jobId = jobId;
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
        List<JobScalingRule> rules;
    }

    @EqualsAndHashCode(callSuper = true)
    @SuperBuilder
    @Value
    public static class DeleteScalerRuleResponse extends BaseResponse  {
    }

    @EqualsAndHashCode(callSuper = true)
    @SuperBuilder
    @Value
    public static class GetJobScalerRuleStreamSubjectResponse extends BaseResponse {
        BehaviorSubject<JobScalerRuleInfo> jobScalerRuleStreamBehaviorSubject;
    }

    @EqualsAndHashCode(callSuper = true)
    @SuperBuilder
    @Value
    public static class GetJobScalerRuleStreamResponse extends BaseResponse {
        Observable<JobScalerRuleInfo> scalerRuleObs;
    }
}
