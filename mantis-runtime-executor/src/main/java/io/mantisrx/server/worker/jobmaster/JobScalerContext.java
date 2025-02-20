package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import lombok.Builder;
import lombok.Value;
import rx.functions.Action0;
import rx.functions.Action1;

@Builder
@Value
public class JobScalerContext {
    String jobId;
    SchedulingInfo schedInfo;
    WorkerMetricsClient workerMetricsClient;
    AutoScaleMetricsConfig autoScaleMetricsConfig;
    MantisMasterGateway masterClientApi;
    Context context;
    Action0 observableOnCompleteCallback;
    Action1<Throwable> observableOnErrorCallback;
    Action0 observableOnTerminateCallback;
    JobAutoscalerManager jobAutoscalerManager;
}
