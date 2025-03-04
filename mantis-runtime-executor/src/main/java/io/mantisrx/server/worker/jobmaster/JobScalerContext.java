package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.server.worker.jobmaster.rules.RuleUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import rx.functions.Action0;
import rx.functions.Action1;

import java.util.Comparator;
import java.util.function.BiFunction;


@Builder
@Value
public class JobScalerContext {
    @NonNull
    String jobId;

    // wrap existing arguments to v1 job auto scaler
    SchedulingInfo schedInfo;
    WorkerMetricsClient workerMetricsClient;
    AutoScaleMetricsConfig autoScaleMetricsConfig;
    MantisMasterGateway masterClientApi;
    Context context;
    Action0 observableOnCompleteCallback;
    Action1<Throwable> observableOnErrorCallback;
    Action0 observableOnTerminateCallback;
    JobAutoscalerManager jobAutoscalerManager;

    // injected functions
    @NonNull
    @Builder.Default
    BiFunction<JobScalerContext, JobScalingRule, JobAutoScalerService> jobAutoScalerServiceFactory =
        JobMasterService::new;

    @NonNull
    @Builder.Default
    Comparator<String> ruleIdComparator = RuleUtils.defaultIntValueRuleIdComparator();
}
