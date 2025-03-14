package io.mantisrx.master.jobcluster.scaler;

import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;

@FunctionalInterface
public interface JobClusterScalerRuleDataFactory {
    IJobClusterScalerRuleData create(String jobClusterName);
}
