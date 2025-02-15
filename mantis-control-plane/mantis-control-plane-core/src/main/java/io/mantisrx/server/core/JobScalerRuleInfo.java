package io.mantisrx.server.core;

import io.mantisrx.runtime.codec.JsonType;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nullable;
import java.util.List;

@Builder
@Value
public class JobScalerRuleInfo implements JsonType {
    public static final String HB_JobId = "HB_JobId";
    public static final String SendHBParam = "sendHB";

    String jobId;
    boolean jobCompleted;

    @Nullable
    List<JobScalingRule> rules;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobScalerRuleInfo(@JsonProperty("jobId") String jobId,
                             @JsonProperty("jobCompleted") boolean jobCompleted,
                             @JsonProperty("rules") List<JobScalingRule> rules) {
        this.jobId = jobId;
        this.jobCompleted = jobCompleted;
        this.rules = rules;
    }
}
