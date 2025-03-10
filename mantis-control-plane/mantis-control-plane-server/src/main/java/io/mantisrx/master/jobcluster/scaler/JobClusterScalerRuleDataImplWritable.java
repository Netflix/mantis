package io.mantisrx.master.jobcluster.scaler;

import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the data contract for job cluster level scaling rules. For simplicity the entity is immutable and share
 * the same class with persistence writer.
 *
 * [Note] Make sure to add @JsonIgnore annotation to any getter method not mean to be serialized.
 */
@Value
public class JobClusterScalerRuleDataImplWritable implements IJobClusterScalerRuleData {

    String jobClusterName;
    long lastRuleIdNumber;
    boolean disabled;
    List<JobClusterScalerRule> scalerRules;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown=true)
    public JobClusterScalerRuleDataImplWritable(
        @JsonProperty("jobClusterName")String jobClusterName,
        @JsonProperty("lastRuleIdNumber")long lastRuleIdNumber,
        @JsonProperty("disabled")boolean disabled,
        @JsonProperty("scalerRules")List<JobClusterScalerRule> scalerRules) {
        this.jobClusterName = jobClusterName;
        this.lastRuleIdNumber = lastRuleIdNumber;
        this.disabled = disabled;
        this.scalerRules = scalerRules == null ? ImmutableList.of() : scalerRules;
    }

    public static IJobClusterScalerRuleData of(String jobClusterName) {
        return new JobClusterScalerRuleDataImplWritable(jobClusterName, 0, false, new ArrayList<>());
    }

    @Override
    public IJobClusterScalerRuleData merge(JobClusterScalerRuleProto.CreateScalerRuleRequest scalerRuleReq) {
        List<JobClusterScalerRule> mergedRules = ImmutableList.<JobClusterScalerRule>builder()
            .addAll(this.scalerRules)
            .add(JobClusterScalerRule.fromProto(scalerRuleReq, this.lastRuleIdNumber))
            .build();
        return new JobClusterScalerRuleDataImplWritable(
            jobClusterName,
            this.lastRuleIdNumber + 1,
            false,
            mergedRules);
    }

    @Override
    public IJobClusterScalerRuleData delete(String ruleId) {
        if (this.scalerRules.stream().anyMatch(r -> r.getRule().getRuleId().equals(ruleId))) {
            List<JobClusterScalerRule> mergedRules = ImmutableList.<JobClusterScalerRule>builder()
                .addAll(
                    this.scalerRules.stream().filter(r -> !r.getRule().getRuleId().equals(ruleId)).iterator())
                .build();
            return new JobClusterScalerRuleDataImplWritable(
                jobClusterName,
                this.lastRuleIdNumber,
                false,
                mergedRules);
        }
        else {
            return this;
        }
    }

    @JsonIgnore
    @Override
    public List<JobScalingRule> getProtoRules() {
        return this.scalerRules.stream().map(JobClusterScalerRule::toProto).collect(ImmutableList.toImmutableList());
    }
}
