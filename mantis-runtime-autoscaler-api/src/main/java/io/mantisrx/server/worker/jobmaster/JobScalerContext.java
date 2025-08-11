/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.server.worker.jobmaster;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
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
