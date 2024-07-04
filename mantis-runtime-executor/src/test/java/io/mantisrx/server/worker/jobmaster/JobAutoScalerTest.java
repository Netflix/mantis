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

import static io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason.DataDrop;
import static io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason.KafkaLag;
import static io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason.UserDefined;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import io.mantisrx.server.worker.jobmaster.clutch.ClutchConfiguration;
import io.mantisrx.server.worker.jobmaster.clutch.rps.ClutchRpsPIDConfig;
import io.vavr.Tuple;
import io.vavr.control.Option;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Func1;


public class JobAutoScalerTest {

    private static final Logger logger = LoggerFactory.getLogger(JobAutoScalerTest.class);

    @Test
    public void testScaleUp() throws InterruptedException {
        final String jobId = "test-job-1";
        final int coolDownSec = 2;
        final int scalingStageNum = 1;
        final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);
        final Map<Integer, StageSchedulingInfo> schedulingInfoMap = new HashMap<>();
        final int numStage1Workers = 1;
        final int increment = 1;
        final int decrement = 1;
        final int min = 1;
        final int max = 5;
        final double scaleUpAbovePct = 45.0;
        final double scaleDownBelowPct = 15.0;
        final double workerMemoryMB = 512.0;

        final StageSchedulingInfo stage1SchedInfo = StageSchedulingInfo.builder()
                .numberOfInstances(numStage1Workers)
                .machineDefinition(new MachineDefinition(2, workerMemoryMB, 200, 1024, 2))
                .scalingPolicy(new StageScalingPolicy(scalingStageNum, min, max, increment, decrement, coolDownSec,
                    Collections.singletonMap(StageScalingPolicy.ScalingReason.Memory,
                        new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, scaleDownBelowPct, scaleUpAbovePct, new StageScalingPolicy.RollingCount(1, 2))), false))
                .scalable(true)
                .build();

        schedulingInfoMap.put(scalingStageNum, stage1SchedInfo);

        when(mockMasterClientApi.scaleJobStage(eq(jobId), eq(scalingStageNum), eq(numStage1Workers + increment), anyString())).thenReturn(Observable.just(true));

        Context context = mock(Context.class);
        when(context.getWorkerMapObservable()).thenReturn(Observable.empty());

        final JobAutoScaler jobAutoScaler = new JobAutoScaler(jobId, new SchedulingInfo(schedulingInfoMap), mockMasterClientApi, context, JobAutoscalerManager.DEFAULT);
        jobAutoScaler.start();
        final Observer<JobAutoScaler.Event> jobAutoScalerObserver = jobAutoScaler.getObserver();

        // should trigger a scale up (above 45% scaleUp threshold)
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleUpAbovePct / 100.0 + 0.01), (scaleUpAbovePct / 100.0 + 0.01) * 100.0, numStage1Workers));

        verify(mockMasterClientApi, timeout(1000).times(1)).scaleJobStage(jobId, scalingStageNum, numStage1Workers + increment, String.format("Memory with value %1$,.2f exceeded scaleUp threshold of 45.0", (scaleUpAbovePct / 100.0 + 0.01) * 100.0));

        // should *not* trigger a scale up before cooldown period (above 45% scaleUp threshold)
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleUpAbovePct / 100.0 + 0.01), scaleUpAbovePct + 0.01, numStage1Workers + increment));
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleUpAbovePct / 100.0 + 0.01), scaleUpAbovePct + 0.01, numStage1Workers + increment));

        Thread.sleep(coolDownSec * 1000);

        // retry sending auto scale event till scaleJobStage request sent to master, as there is possible a race between the sleep for coolDownSecs in the Test and the event being processed before coolDownSecs
        final CountDownLatch retryLatch = new CountDownLatch(1);

        when(mockMasterClientApi.scaleJobStage(eq(jobId), eq(scalingStageNum), eq(numStage1Workers + 2 * increment), anyString())).thenAnswer(new Answer<Observable<Void>>() {
            @Override
            public Observable<Void> answer(InvocationOnMock invocation) throws Throwable {
                retryLatch.countDown();
                return Observable.just(null);
            }
        });
        do {
            logger.info("sending Job auto scale Event");
            // should trigger a scale up after cooldown period (above 45% scaleUp threshold)
            jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleUpAbovePct / 100.0 + 0.01), (scaleUpAbovePct / 100.0 + 0.01) * 100.0, numStage1Workers + increment));
        } while (!retryLatch.await(1, TimeUnit.SECONDS));

        verify(mockMasterClientApi, timeout(1000).times(1)).scaleJobStage(jobId, scalingStageNum, numStage1Workers + 2 * increment, String.format("Memory with value %1$,.2f exceeded scaleUp threshold of 45.0", (scaleUpAbovePct / 100.0 + 0.01) * 100.0));
    }

    @Test
    public void testScalingResiliency() throws InterruptedException {
        final String jobId = "test-job-1";
        final int coolDownSec = 2;
        final int scalingStageNum = 1;
        final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);
        final Map<Integer, StageSchedulingInfo> schedulingInfoMap = new HashMap<>();
        final int numStage1Workers = 1;
        final int increment = 1;
        final int decrement = 1;
        final int min = 1;
        final int max = 5;
        final double scaleUpAbovePct = 45.0;
        final double scaleDownBelowPct = 15.0;
        final double workerMemoryMB = 512.0;

        final StageSchedulingInfo stage1SchedInfo = StageSchedulingInfo.builder()
                .numberOfInstances(numStage1Workers)
                .machineDefinition(new MachineDefinition(2, workerMemoryMB, 200, 1024, 2))
                .scalingPolicy(new StageScalingPolicy(scalingStageNum, min, max, increment, decrement, coolDownSec,
                    new HashMap<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy>() {
                        {
                            put(StageScalingPolicy.ScalingReason.Memory, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, scaleDownBelowPct, scaleUpAbovePct, new StageScalingPolicy.RollingCount(1, 2)));
                            put(StageScalingPolicy.ScalingReason.AutoscalerManagerEvent, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.AutoscalerManagerEvent, 0.0, 0.0, new StageScalingPolicy.RollingCount(1, 2)));
                        }
                    }, false))
                .scalable(true)
                .build();

        schedulingInfoMap.put(scalingStageNum, stage1SchedInfo);

        final CountDownLatch scaleJobStageSuccessLatch = new CountDownLatch(1);
        final AtomicInteger count = new AtomicInteger(0);
        final Observable<Boolean> simulateScaleJobStageFailureResp = Observable.just(1).map(new Func1<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) {
                if (count.incrementAndGet() < 3) {
                    throw new IllegalStateException("fake connection exception");
                } else {
                    scaleJobStageSuccessLatch.countDown();
                    return true;
                }
            }
        });
        when(mockMasterClientApi.scaleJobStage(eq(jobId), eq(scalingStageNum), eq(numStage1Workers + increment), anyString())).thenReturn(simulateScaleJobStageFailureResp);

        Context context = mock(Context.class);
        when(context.getWorkerMapObservable()).thenReturn(Observable.empty());

        final JobAutoScaler jobAutoScaler = new JobAutoScaler(jobId, new SchedulingInfo(schedulingInfoMap), mockMasterClientApi, context, JobAutoscalerManager.DEFAULT);
        jobAutoScaler.start();
        final Observer<JobAutoScaler.Event> jobAutoScalerObserver = jobAutoScaler.getObserver();

        // should trigger a scale up (above 45% scaleUp threshold)
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleUpAbovePct / 100.0 + 0.01), (scaleUpAbovePct / 100.0 + 0.01) * 100.0, numStage1Workers));

        verify(mockMasterClientApi, timeout(1000).times(1)).scaleJobStage(jobId, scalingStageNum, numStage1Workers + increment, String.format("Memory with value %1$,.2f exceeded scaleUp threshold of 45.0", (scaleUpAbovePct / 100.0 + 0.01) * 100.0));

        scaleJobStageSuccessLatch.await();
    }

    @Test
    public void testScaleDown() throws InterruptedException {
        final String jobId = "test-job-1";
        final int coolDownSec = 2;
        final int scalingStageNum = 1;
        final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);
        final Map<Integer, StageSchedulingInfo> schedulingInfoMap = new HashMap<>();
        final int numStage1Workers = 2;
        final int increment = 1;
        final int decrement = 1;
        final int min = 1;
        final int max = 5;
        final double scaleUpAbovePct = 45.0;
        final double scaleDownBelowPct = 15.0;
        final double workerMemoryMB = 512.0;

        final StageSchedulingInfo stage1SchedInfo = StageSchedulingInfo.builder()
                .numberOfInstances(numStage1Workers)
                .machineDefinition(new MachineDefinition(2, workerMemoryMB, 200, 1024, 2))
                .scalingPolicy(new StageScalingPolicy(scalingStageNum, min, max, increment, decrement, coolDownSec,
                    Collections.singletonMap(StageScalingPolicy.ScalingReason.Memory,
                        new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, scaleDownBelowPct, scaleUpAbovePct, new StageScalingPolicy.RollingCount(1, 2))), false))
                .scalable(true)
                .build();

        schedulingInfoMap.put(scalingStageNum, stage1SchedInfo);

        when(mockMasterClientApi.scaleJobStage(eq(jobId), eq(scalingStageNum), eq(numStage1Workers - decrement), anyString())).thenReturn(Observable.just(true));

        Context context = mock(Context.class);
        when(context.getWorkerMapObservable()).thenReturn(Observable.empty());

        final JobAutoScaler jobAutoScaler = new JobAutoScaler(jobId, new SchedulingInfo(schedulingInfoMap), mockMasterClientApi, context, JobAutoscalerManager.DEFAULT);
        jobAutoScaler.start();
        final Observer<JobAutoScaler.Event> jobAutoScalerObserver = jobAutoScaler.getObserver();

        // should trigger a scale down (below 15% scaleDown threshold)
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), (scaleDownBelowPct / 100.0 - 0.01) * 100.0, numStage1Workers));

        verify(mockMasterClientApi, timeout(1000).times(1)).scaleJobStage(jobId, scalingStageNum, numStage1Workers - decrement, String.format("Memory with value %1$,.2f is below scaleDown threshold of %2$,.1f", (scaleDownBelowPct / 100.0 - 0.01) * 100.0, scaleDownBelowPct));

        // should *not* trigger a scale down before cooldown period (below 15% scaleDown threshold)
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), scaleDownBelowPct - 0.01,  numStage1Workers - decrement));
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), scaleDownBelowPct - 0.01, numStage1Workers - decrement));

        Thread.sleep(coolDownSec * 1000);

        if (numStage1Workers - decrement == min) {
            // should not trigger a scale down after cooldown period if numWorkers=min (below 15% scaleDown threshold)
            jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), scaleDownBelowPct - 0.01, numStage1Workers - decrement));
            verifyNoMoreInteractions(mockMasterClientApi);
        }
    }

    @Test
    public void testScaleDownManagerDisabled() throws InterruptedException {
        final String jobId = "test-job-1";
        final int coolDownSec = 2;
        final int scalingStageNum = 1;
        final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);
        final Map<Integer, StageSchedulingInfo> schedulingInfoMap = new HashMap<>();
        final int numStage1Workers = 2;
        final int increment = 1;
        final int decrement = 1;
        final int min = 1;
        final int max = 5;
        final double scaleUpAbovePct = 45.0;
        final double scaleDownBelowPct = 15.0;
        final double workerMemoryMB = 512.0;

        final StageSchedulingInfo stage1SchedInfo = StageSchedulingInfo.builder()
            .numberOfInstances(numStage1Workers)
            .machineDefinition(new MachineDefinition(2, workerMemoryMB, 200, 1024, 2))
            .scalingPolicy(new StageScalingPolicy(scalingStageNum, min, max, increment, decrement, coolDownSec,
                Collections.singletonMap(StageScalingPolicy.ScalingReason.Memory,
                    new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, scaleDownBelowPct, scaleUpAbovePct, new StageScalingPolicy.RollingCount(1, 2))), true))
            .scalable(true)
            .build();

        schedulingInfoMap.put(scalingStageNum, stage1SchedInfo);

        when(mockMasterClientApi.scaleJobStage(eq(jobId), eq(scalingStageNum), eq(numStage1Workers - decrement), anyString())).thenReturn(Observable.just(true));

        Context context = mock(Context.class);
        when(context.getWorkerMapObservable()).thenReturn(Observable.empty());

        JobAutoscalerManager scalarManager = new JobAutoscalerManager() {
            @Override
            public boolean isScaleDownEnabled() {
                return false;
            }
        };
        final JobAutoScaler jobAutoScaler = new JobAutoScaler(jobId, new SchedulingInfo(schedulingInfoMap), mockMasterClientApi, context, scalarManager);
        jobAutoScaler.start();
        final Observer<JobAutoScaler.Event> jobAutoScalerObserver = jobAutoScaler.getObserver();

        // should trigger a scale down (below 15% scaleDown threshold)
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), (scaleDownBelowPct / 100.0 - 0.01) * 100.0, numStage1Workers));

        verify(mockMasterClientApi, timeout(1000).times(0)).scaleJobStage(jobId, scalingStageNum, numStage1Workers - decrement, String.format("Memory with value %1$,.2f is below scaleDown threshold of %2$,.1f", (scaleDownBelowPct / 100.0 - 0.01) * 100.0, scaleDownBelowPct));

        // should *not* trigger a scale down before cooldown period (below 15% scaleDown threshold)
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), scaleDownBelowPct - 0.01,  numStage1Workers - decrement));
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), scaleDownBelowPct - 0.01, numStage1Workers - decrement));

        Thread.sleep(coolDownSec * 1000);

        if (numStage1Workers - decrement == min) {
            // should not trigger a scale down after cooldown period if numWorkers=min (below 15% scaleDown threshold)
            jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), scaleDownBelowPct - 0.01, numStage1Workers - decrement));
            verifyNoMoreInteractions(mockMasterClientApi);
        }
    }

    @Test
    public void testScaleDownNotLessThanMin() throws InterruptedException {
        final String jobId = "test-job-1";
        final int coolDownSec = 2;
        final int scalingStageNum = 1;
        final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);
        final Map<Integer, StageSchedulingInfo> schedulingInfoMap = new HashMap<>();
        final int numStage1Workers = 5;
        final int increment = 10;
        // decrement by 10 on scale down, this will push num workers below min and below 0.
        final int decrement = 10;
        final int min = 3;
        final int max = 50;
        final double scaleUpAbovePct = 45.0;
        final double scaleDownBelowPct = 15.0;
        final double workerMemoryMB = 512.0;

        final StageSchedulingInfo stage1SchedInfo = StageSchedulingInfo.builder()
                .numberOfInstances(numStage1Workers).machineDefinition(new MachineDefinition(2, workerMemoryMB, 200, 1024, 2))
                .scalingPolicy(new StageScalingPolicy(scalingStageNum, min, max, increment, decrement, coolDownSec,
                    Collections.singletonMap(StageScalingPolicy.ScalingReason.Memory,
                        new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, scaleDownBelowPct, scaleUpAbovePct, new StageScalingPolicy.RollingCount(1, 2))), false))
                .scalable(true)
                .build();

        schedulingInfoMap.put(scalingStageNum, stage1SchedInfo);

        when(mockMasterClientApi.scaleJobStage(eq(jobId), eq(scalingStageNum), anyInt(), anyString())).thenReturn(Observable.just(true));

        Context context = mock(Context.class);
        when(context.getWorkerMapObservable()).thenReturn(Observable.empty());

        final JobAutoScaler jobAutoScaler = new JobAutoScaler(jobId, new SchedulingInfo(schedulingInfoMap), mockMasterClientApi, context, JobAutoscalerManager.DEFAULT);
        jobAutoScaler.start();
        final Observer<JobAutoScaler.Event> jobAutoScalerObserver = jobAutoScaler.getObserver();

        // should trigger a scale down (below 15% scaleDown threshold)
        jobAutoScalerObserver.onNext(new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.Memory, scalingStageNum, workerMemoryMB * (scaleDownBelowPct / 100.0 - 0.01), (scaleDownBelowPct / 100.0 - 0.01) * 100.0, numStage1Workers));

        verify(mockMasterClientApi, timeout(1000).times(1)).scaleJobStage(jobId, scalingStageNum, min, String.format("Memory with value %1$,.2f is below scaleDown threshold of %2$,.1f", (scaleDownBelowPct / 100.0 - 0.01) * 100.0, scaleDownBelowPct));
        verifyNoMoreInteractions(mockMasterClientApi);
    }

    @Test
    public void testScaleUpOnDifferentScalingReasons() throws InterruptedException {
        final List<StageScalingPolicy.ScalingReason> scalingReasons = Arrays.asList(DataDrop, KafkaLag, UserDefined);
        for (StageScalingPolicy.ScalingReason scalingReason : scalingReasons) {
            logger.info("==== test scaling reason {} =====", scalingReason.name());
            final String jobId = "test-job-1";
            final int coolDownSec = 2;
            final int scalingStageNum = 1;
            final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);
            final Map<Integer, StageSchedulingInfo> schedulingInfoMap = new HashMap<>();
            final int numStage1Workers = 1;
            final int increment = 1;
            final int decrement = 0;
            final int min = 1;
            final int max = 5;
            final double scaleUpAbove = 2000.0;
            final double scaleDownBelow = 0.0;
            final double workerMemoryMB = 512.0;

            final StageSchedulingInfo stage1SchedInfo = StageSchedulingInfo.builder()
                    .numberOfInstances(numStage1Workers)
                    .machineDefinition(new MachineDefinition(2, workerMemoryMB, 200, 1024, 2))
                    .scalingPolicy(new StageScalingPolicy(scalingStageNum, min, max, increment, decrement, coolDownSec,
                        Collections.singletonMap(scalingReason,
                            new StageScalingPolicy.Strategy(scalingReason, scaleDownBelow, scaleUpAbove, new StageScalingPolicy.RollingCount(1, 2))), false))
                    .scalable(true)
                    .build();

            schedulingInfoMap.put(scalingStageNum, stage1SchedInfo);

            when(mockMasterClientApi.scaleJobStage(eq(jobId), eq(scalingStageNum), eq(numStage1Workers + increment), anyString())).thenReturn(Observable.just(true));

            Context context = mock(Context.class);
            when(context.getWorkerMapObservable()).thenReturn(Observable.empty());

            final JobAutoScaler jobAutoScaler = new JobAutoScaler(jobId, new SchedulingInfo(schedulingInfoMap), mockMasterClientApi, context, JobAutoscalerManager.DEFAULT);
            jobAutoScaler.start();
            final Observer<JobAutoScaler.Event> jobAutoScalerObserver = jobAutoScaler.getObserver();

            // should trigger a scale up (above scaleUp threshold)
            jobAutoScalerObserver.onNext(new JobAutoScaler.Event(scalingReason, scalingStageNum, scaleUpAbove + 0.01, scaleUpAbove + 0.01, numStage1Workers));

            verify(mockMasterClientApi, timeout(1000).times(1)).scaleJobStage(jobId, scalingStageNum, numStage1Workers + increment, String.format("%s with value %2$.2f exceeded scaleUp threshold of %3$.1f", scalingReason.name(), (scaleUpAbove + 0.01), scaleUpAbove));

            // should *not* trigger a scale up before cooldown period (above scaleUp threshold)
            jobAutoScalerObserver.onNext(new JobAutoScaler.Event(scalingReason, scalingStageNum, scaleUpAbove + 0.01, scaleUpAbove + 0.01, numStage1Workers + increment));
            jobAutoScalerObserver.onNext(new JobAutoScaler.Event(scalingReason, scalingStageNum, scaleUpAbove + 0.01, scaleUpAbove + 0.01, numStage1Workers + increment));

            Thread.sleep(coolDownSec * 1000);

            // retry sending auto scale event till scaleJobStage request sent to master, as there is possible a race between the sleep for coolDownSecs in the Test and the event being processed before coolDownSecs
            final CountDownLatch retryLatch = new CountDownLatch(1);
            when(mockMasterClientApi.scaleJobStage(eq(jobId), eq(scalingStageNum), eq(numStage1Workers + 2 * increment), anyString())).thenAnswer(new Answer<Observable<Void>>() {
                @Override
                public Observable<Void> answer(InvocationOnMock invocation) throws Throwable {
                    retryLatch.countDown();
                    return Observable.just(null);
                }
            });

            do {
                logger.info("sending Job auto scale Event");
                // should trigger a scale up after cooldown period (above scaleUp threshold)
                jobAutoScalerObserver.onNext(new JobAutoScaler.Event(scalingReason, scalingStageNum, scaleUpAbove + 0.01, scaleUpAbove + 0.01, numStage1Workers + increment));
            } while (!retryLatch.await(1, TimeUnit.SECONDS));

            verify(mockMasterClientApi, timeout(1000).times(1)).scaleJobStage(jobId, scalingStageNum, numStage1Workers + 2 * increment, String.format("%s with value %2$.2f exceeded scaleUp threshold of %3$.1f", scalingReason.name(), (scaleUpAbove + 0.01), scaleUpAbove));
        }
    }

    @Test
    public void testGetClutchConfigurationFromJson() throws Exception {
        String json = "{" +
                "  \"cooldownSeconds\": 100," +
                "  \"integralDecay\": 0.7," +
                "  \"rpsConfig\": {" +
                "    \"scaleUpAbovePct\": 30.0," +
                "    \"scaleUpMultiplier\": 1.5" +
                "  }" +
                "}";
        final JobAutoScaler jobAutoScaler = new JobAutoScaler("jobId", null, null, null, JobAutoscalerManager.DEFAULT);
        ClutchConfiguration config = jobAutoScaler.getClutchConfiguration(json).get(1);

        ClutchRpsPIDConfig expected = new ClutchRpsPIDConfig(0.0, Tuple.of(30.0, 0.0), 0.0, 0.0, Option.of(75.0), Option.of(30.0), Option.of(0.0), Option.of(1.5), Option.of(1.0));
        assertEquals(Option.of(100L), config.getCooldownSeconds());
        assertEquals(0.7, config.getIntegralDecay().get(), 1e-10);
        assertEquals(expected, config.getRpsConfig().get());

        json = "{" +
                "  \"cooldownSeconds\": 100," +
                "  \"rpsConfig\": {" +
                "    \"rope\": [90.0, 80.0]," +
                "    \"setPointPercentile\": 95.0," +
                "    \"scaleDownBelowPct\": 150.0," +
                "    \"scaleDownMultiplier\": 0.5" +
                "  }" +
                "}";
        config = jobAutoScaler.getClutchConfiguration(json).get(1);
        expected = new ClutchRpsPIDConfig(0.0, Tuple.of(90.0, 80.0), 0.0, 0.0, Option.of(95.0), Option.of(0.0), Option.of(150.0), Option.of(1.0), Option.of(0.5));
        assertEquals(expected, config.getRpsConfig().get());

        json = "{" +
                "  \"cooldownSeconds\": 100" +
                "}";
        config = jobAutoScaler.getClutchConfiguration(json).get(1);
        assertFalse(config.getRpsConfig().isDefined());
        assertEquals(0, config.getMinSize());
    }

    //    @Test
    //    public void testBackPressure() throws InterruptedException {
    //        //  DropOperator does not propogate backpressure in this scenario
    //        final CountDownLatch latch = new CountDownLatch(1000);
    //        Observable.range(1, 1000)
    //            .subscribeOn(Schedulers.computation())
    //            .onBackpressureBuffer(100, () -> System.out.printf("overflow"), BackpressureOverflow.ON_OVERFLOW_DROP_OLDEST)
    ////            .lift(new DropOperator<Integer>("op1"))
    ////            .lift(new DropOperator<Integer>("op2"))
    //            .observeOn(Schedulers.io())
    //            .map(new Func1<Integer, Integer>() {
    //                @Override
    //                public Integer call(Integer integer) {
    //                    try {
    //                        Thread.sleep(10);
    //                        System.out.println("got "+integer);
    //                        latch.countDown();
    //                    } catch (InterruptedException e) {
    //                        e.printStackTrace();
    //                    }
    //                    return integer;
    //                }
    //            })
    //            .subscribe();
    //
    //        latch.await();
    //    }

}
