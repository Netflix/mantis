package io.mantisrx.master.jobcluster.scaler;

import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;
import io.mantisrx.runtime.descriptor.JobScalingRule;

import java.util.List;

public interface IJobClusterScalerRuleData {
    List<JobClusterScalerRule> getScalerRules();

    String getJobClusterName();

    long getLastRuleIdNumber();

    boolean isDisabled();

    IJobClusterScalerRuleData merge(JobClusterScalerRuleProto.CreateScalerRuleRequest scalerRuleReq);
    IJobClusterScalerRuleData delete(String ruleId);
    List<JobScalingRule> getProtoRules();
}
