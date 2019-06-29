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

import static io.mantisrx.server.core.stats.MetricStringConstants.DATA_DROP_METRIC_GROUP;
import static io.mantisrx.server.core.stats.MetricStringConstants.KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.mantisrx.common.metrics.measurement.GaugeMeasurement;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerAssignments;
import io.mantisrx.server.core.WorkerHost;
import io.mantisrx.server.core.stats.MetricStringConstants;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.functions.Func1;


public class WorkerMetricHandlerTest {

    private static final Logger logger = LoggerFactory.getLogger(WorkerMetricHandlerTest.class);

    @Test
    public void testDropDataMetricTriggersAutoScale() throws InterruptedException {
        final String jobId = "test-job-1";
        final int stage = 1;
        final int workerIdx = 0;
        final int workerNum = 1;
        final int dropCount = 1;
        final int onNextCount = 9;
        final double dropPercent = dropCount * 100.0 / (dropCount + onNextCount);

        final List<GaugeMeasurement> gauges = Arrays.asList(
                new GaugeMeasurement(MetricStringConstants.ON_NEXT_COUNT, onNextCount),
                new GaugeMeasurement(MetricStringConstants.DROP_COUNT, dropCount));

        final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);

        final Map<Integer, WorkerAssignments> assignmentsMap = new HashMap<>();
        assignmentsMap.put(stage, new WorkerAssignments(stage, 1,
                Collections.singletonMap(1, new WorkerHost("localhost", workerIdx, Arrays.asList(31300), MantisJobState.Started, workerNum, 31301, -1))));
        when(mockMasterClientApi.schedulingChanges(jobId)).thenReturn(Observable.just(new JobSchedulingInfo(jobId, assignmentsMap)));

        final CountDownLatch latch = new CountDownLatch(1);

        final AutoScaleMetricsConfig aggregationConfig = new AutoScaleMetricsConfig();

        final WorkerMetricHandler workerMetricHandler = new WorkerMetricHandler(jobId, new Observer<JobAutoScaler.Event>() {
            @Override
            public void onCompleted() {
                logger.warn("onCompleted");
            }

            @Override
            public void onError(Throwable e) {
                logger.warn("onError {}", e.getMessage(), e);
            }

            @Override
            public void onNext(JobAutoScaler.Event event) {
                logger.info("got auto scale event {}", event);
                JobAutoScaler.Event expected = new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.DataDrop, stage, dropPercent, 1, "");
                assertEquals(expected, event);
                latch.countDown();
            }
        }, mockMasterClientApi, aggregationConfig);

        final Observer<MetricData> metricDataObserver = workerMetricHandler.initAndGetMetricDataObserver();

        metricDataObserver.onNext(new MetricData(jobId, stage, workerIdx, workerNum, DATA_DROP_METRIC_GROUP, gauges));

        assertTrue(latch.await(30 + 5/* leeway */, TimeUnit.SECONDS));
    }

    @Test
    public void testKafkaLagAndUserDefinedTriggersAutoScale() throws InterruptedException {
        final String jobId = "test-job-1";
        final int stage = 1;
        final int workerIdx = 0;
        final int workerNum = 1;
        final int kafkaLag = 10_000;
        final int numWorkers = 2;

        final int metricValue = 1_000;

        final String testMetricGroup = "testMetricGroup";
        final String testMetricName = "testMetricName";

        final List<GaugeMeasurement> worker1KafkaGauges = Arrays.asList(
                new GaugeMeasurement(MetricStringConstants.KAFKA_LAG, kafkaLag / 2));
        final List<GaugeMeasurement> worker2KafkaGauges = Arrays.asList(
                new GaugeMeasurement(MetricStringConstants.KAFKA_LAG, kafkaLag));

        final List<GaugeMeasurement> gauges1 = Arrays.asList(
                new GaugeMeasurement(testMetricName, metricValue / 2));
        final List<GaugeMeasurement> gauges2 = Arrays.asList(
                new GaugeMeasurement(testMetricName, metricValue));

        final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);

        final Map<Integer, WorkerAssignments> assignmentsMap = new HashMap<>();
        final Map<Integer, WorkerHost> hosts = new HashMap<>();
        hosts.put(1, new WorkerHost("localhost", workerIdx, Arrays.asList(31300), MantisJobState.Started, workerNum, 31301, -1));
        hosts.put(2, new WorkerHost("localhost", workerIdx + 1, Arrays.asList(31305), MantisJobState.Started, workerNum + 1, 31316, -1));

        assignmentsMap.put(stage, new WorkerAssignments(stage, numWorkers, hosts));
        when(mockMasterClientApi.schedulingChanges(jobId)).thenReturn(Observable.just(new JobSchedulingInfo(jobId, assignmentsMap)));

        final CountDownLatch latch = new CountDownLatch(2);

        final AutoScaleMetricsConfig aggregationConfig = new AutoScaleMetricsConfig(Collections.singletonMap(testMetricGroup, Collections.singletonMap(testMetricName, AutoScaleMetricsConfig.AggregationAlgo.AVERAGE)));

        final WorkerMetricHandler workerMetricHandler = new WorkerMetricHandler(jobId, new Observer<JobAutoScaler.Event>() {
            @Override
            public void onCompleted() {
                logger.warn("onCompleted");
            }

            @Override
            public void onError(Throwable e) {
                logger.warn("onError {}", e.getMessage(), e);
            }

            @Override
            public void onNext(JobAutoScaler.Event event) {
                logger.info("got auto scale event {}", event);
                final long count = latch.getCount();
                if (count == 2) {
                    JobAutoScaler.Event expected1 = new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.UserDefined, stage, metricValue * 3 / 4, numWorkers, "");
                    assertEquals(expected1, event);
                    latch.countDown();
                }
                if (count == 1) {
                    JobAutoScaler.Event expected2 = new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.KafkaLag, stage, kafkaLag, numWorkers, "");
                    assertEquals(expected2, event);
                    latch.countDown();
                }
            }
        }, mockMasterClientApi, aggregationConfig);

        final Observer<MetricData> metricDataObserver = workerMetricHandler.initAndGetMetricDataObserver();

        metricDataObserver.onNext(new MetricData(jobId, stage, workerIdx, workerNum, KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP, worker1KafkaGauges));
        metricDataObserver.onNext(new MetricData(jobId, stage, workerIdx + 1, workerNum + 1, KAFKA_CONSUMER_FETCH_MGR_METRIC_GROUP, worker2KafkaGauges));
        metricDataObserver.onNext(new MetricData(jobId, stage, workerIdx, workerNum, testMetricGroup, gauges1));
        metricDataObserver.onNext(new MetricData(jobId, stage, workerIdx + 1, workerNum + 1, testMetricGroup, gauges2));

        assertTrue(latch.await(30 + 5/* leeway */, TimeUnit.SECONDS));
    }

    @Test
    public void testOutlierResubmitWorks() throws InterruptedException {
        final String jobId = "test-job-1";
        final int stage = 1;
        final int workerIdx = 0;
        final int workerNum = 1;
        final int numWorkers = 3;
        final int dropCount = 1;
        final int onNextCount = 9;
        final double dropPercent = dropCount * 100.0 / (dropCount + onNextCount);

        final List<GaugeMeasurement> outlierDropGauges = Arrays.asList(
                new GaugeMeasurement(MetricStringConstants.ON_NEXT_COUNT, onNextCount),
                new GaugeMeasurement(MetricStringConstants.DROP_COUNT, dropCount));

        final List<GaugeMeasurement> zeroDropGauges = Arrays.asList(
                new GaugeMeasurement(MetricStringConstants.ON_NEXT_COUNT, onNextCount),
                new GaugeMeasurement(MetricStringConstants.DROP_COUNT, 0));

        final MantisMasterClientApi mockMasterClientApi = mock(MantisMasterClientApi.class);

        final Map<Integer, WorkerAssignments> assignmentsMap = new HashMap<>();

        Map<Integer, WorkerHost> hosts = new HashMap<>();
        hosts.put(workerNum, new WorkerHost("localhost", workerIdx, Arrays.asList(31300), MantisJobState.Started, workerNum, 31301, -1));
        hosts.put(workerNum + 1, new WorkerHost("localhost", workerIdx + 1, Arrays.asList(31302), MantisJobState.Started, workerNum, 31303, -1));
        hosts.put(workerNum + 2, new WorkerHost("localhost", workerIdx + 2, Arrays.asList(31304), MantisJobState.Started, workerNum, 31305, -1));

        assignmentsMap.put(stage, new WorkerAssignments(stage, numWorkers, hosts));

        final CountDownLatch resubmitLatch = new CountDownLatch(1);
        final CountDownLatch autoScaleLatch = new CountDownLatch(1);

        when(mockMasterClientApi.schedulingChanges(jobId)).thenReturn(Observable.just(new JobSchedulingInfo(jobId, assignmentsMap)));
        when(mockMasterClientApi.resubmitJobWorker(anyString(), anyString(), anyInt(), anyString())).thenAnswer(new Answer<Observable<Boolean>>() {
            @Override
            public Observable<Boolean> answer(InvocationOnMock invocation) throws Throwable {

                final Object[] arguments = invocation.getArguments();
                final String jobIdRecv = (String) arguments[0];
                final String user = (String) arguments[1];
                final int resubmittedWorkerNum = (Integer) arguments[2];
                //                final String reason = (String)arguments[3];

                final Observable<Boolean> result = Observable.just(1).map(new Func1<Integer, Boolean>() {
                    @Override
                    public Boolean call(Integer integer) {
                        logger.info("resubmitting worker {} of jobId {}", resubmittedWorkerNum, jobId);
                        assertEquals(workerNum, resubmittedWorkerNum);
                        assertEquals(user, "JobMaster");
                        assertEquals(jobId, jobIdRecv);

                        resubmitLatch.countDown();
                        return true;
                    }
                });
                return result;
            }
        });


        final AutoScaleMetricsConfig aggregationConfig = new AutoScaleMetricsConfig();
        final WorkerMetricHandler workerMetricHandler = new WorkerMetricHandler(jobId, new Observer<JobAutoScaler.Event>() {
            @Override
            public void onCompleted() {
                logger.warn("onCompleted");
            }

            @Override
            public void onError(Throwable e) {
                logger.warn("onError {}", e.getMessage(), e);
            }

            @Override
            public void onNext(JobAutoScaler.Event event) {
                logger.info("got auto scale event {}", event);
                JobAutoScaler.Event expected = new JobAutoScaler.Event(StageScalingPolicy.ScalingReason.DataDrop, stage, dropPercent / numWorkers, numWorkers, "");
                assertEquals(expected, event);
                autoScaleLatch.countDown();
            }
        }, mockMasterClientApi, aggregationConfig);


        final Observer<MetricData> metricDataObserver = workerMetricHandler.initAndGetMetricDataObserver();

        final int minDataPointsForOutlierTrigger = 16;
        for (int i = 0; i <= minDataPointsForOutlierTrigger; i++) {
            metricDataObserver.onNext(new MetricData(jobId, stage, workerIdx, workerNum,
                    DATA_DROP_METRIC_GROUP, outlierDropGauges));
            metricDataObserver.onNext(new MetricData(jobId, stage, workerIdx + 1, workerNum + 1,
                    DATA_DROP_METRIC_GROUP, zeroDropGauges));
            metricDataObserver.onNext(new MetricData(jobId, stage, workerIdx + 2, workerNum + 2,
                    DATA_DROP_METRIC_GROUP, zeroDropGauges));
        }
        assertTrue(resubmitLatch.await(30, TimeUnit.SECONDS));
        assertTrue(autoScaleLatch.await(30 + 5/* leeway */, TimeUnit.SECONDS));
    }

}
