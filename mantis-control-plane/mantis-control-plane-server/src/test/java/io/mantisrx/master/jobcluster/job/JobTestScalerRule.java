package io.mantisrx.master.jobcluster.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.netflix.mantis.master.scheduler.TestHelpers;
import io.mantisrx.master.events.*;
import io.mantisrx.master.jobcluster.proto.JobClusterProto;
import io.mantisrx.master.jobcluster.proto.JobClusterScalerRuleProto;
import io.mantisrx.master.jobcluster.scaler.IJobClusterScalerRuleData;
import io.mantisrx.master.jobcluster.scaler.JobClusterScalerRuleDataImplWritable;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.core.JobCompletedReason;
import io.mantisrx.server.core.JobScalerRuleInfo;
import io.mantisrx.server.master.domain.JobId;
import io.mantisrx.server.master.persistence.MantisJobStore;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@Slf4j
public class JobTestScalerRule {
    static ActorSystem system;
    final LifecycleEventPublisher lifecycleEventPublisher = new LifecycleEventPublisherImpl(new AuditEventSubscriberLoggingImpl(), new StatusEventSubscriberLoggingImpl(), new WorkerEventSubscriberLoggingImpl());

    static Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
    static StageScalingPolicy scalingPolicy;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        TestHelpers.setupMasterConfig();
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    private JobClusterScalerRuleProto.CreateScalerRuleRequest createDummyRequest(String jobClusterName, int desireSize, String cronSchedule) {
        smap.put(StageScalingPolicy.ScalingReason.CPU, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(StageScalingPolicy.ScalingReason.DataDrop, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.DataDrop, 0.0, 2.0, null));
        scalingPolicy = new StageScalingPolicy(1, 1, 2, 1, 1, 60, smap, false);

        JobScalingRule.ScalerConfig scalerConfig =
            JobScalingRule.ScalerConfig.builder()
                .type("standard")
                .stageDesireSize(ImmutableMap.of(1, desireSize))
                .scalingPolicies(ImmutableList.of(scalingPolicy))
                .build();

        JobScalingRule.TriggerConfig triggerConfig =
            JobScalingRule.TriggerConfig.builder()
                .triggerType("cron")
                .scheduleCron(cronSchedule)
                .scheduleDuration("PT1H")
                .customTrigger("none")
                .build();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("key", "value");

        return JobClusterScalerRuleProto.CreateScalerRuleRequest.builder()
            .jobClusterName(jobClusterName)
            .scalerConfig(scalerConfig)
            .triggerConfig(triggerConfig)
            .metadata(metadata)
            .build();
    }

    @Test
    public void testScalerRuleStreamSubject() throws Exception {
        int totalRuleChanges = 3;
        CountDownLatch latch = new CountDownLatch(totalRuleChanges);
        CountDownLatch completeLatch = new CountDownLatch(1);
        List<JobScalerRuleInfo> ruleInfoChangesList = new CopyOnWriteArrayList<>();
        final TestKit probe = new TestKit(system);
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
        SchedulingInfo sInfo = new SchedulingInfo.Builder()
            .numberOfStages(1)
            .multiWorkerScalableStageWithConstraints(1,
                new MachineDefinition(1.0,1.0,1.0,3),
                Lists.newArrayList(),
                Lists.newArrayList(),
                new StageScalingPolicy(1, 0, 10, 1, 1, 0, smap, true))
            .build();

        String clusterName = "testScalerRuleStreamSubject";
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        MantisJobStore jobStoreMock = mock(MantisJobStore.class);
        ActorRef jobActor = JobTestHelper.submitSingleStageScalableJob(system,probe, clusterName, sInfo, schedulerMock, jobStoreMock, lifecycleEventPublisher);

        JobId jobId = new JobId(clusterName, 1);
        JobClusterScalerRuleProto.GetJobScalerRuleStreamRequest getJobScalerRuleStreamRequest =
            new JobClusterScalerRuleProto.GetJobScalerRuleStreamRequest(jobId);

        jobActor.tell(getJobScalerRuleStreamRequest, probe.getRef());
        JobClusterScalerRuleProto.GetJobScalerRuleStreamSubjectResponse resp =
            probe.expectMsgClass(JobClusterScalerRuleProto.GetJobScalerRuleStreamSubjectResponse.class);

        assertEquals(SUCCESS, resp.responseCode);
        assertNotNull(resp.getJobScalerRuleStreamBehaviorSubject());
        BehaviorSubject<JobScalerRuleInfo> scalerRuleSubject = resp.getJobScalerRuleStreamBehaviorSubject();
        scalerRuleSubject.doOnNext((js) -> {
                log.info("scalerRuleSubject Got --> {}", js);
            })
            .filter(Objects::nonNull)
            .doOnCompleted(() -> {
                log.info("rule subject completed with {} changes", ruleInfoChangesList.size());
                completeLatch.countDown();
            })
            .observeOn(Schedulers.io())
            .subscribe((jsi) -> {
                latch.countDown();
                ruleInfoChangesList.add(jsi);
                log.info("adding {} to ruleInfoChangesList", jsi);
            });

        IJobClusterScalerRuleData scalerRuleData1 = JobClusterScalerRuleDataImplWritable.of(jobId.getCluster())
            .merge(createDummyRequest(jobId.getCluster(), 2, "0 0 * * *"));
        jobActor.tell(scalerRuleData1, probe.getRef());

        IJobClusterScalerRuleData scalerRuleData2 = scalerRuleData1
            .merge(createDummyRequest(jobId.getCluster(), 3, "0 1 * * *"));
        jobActor.tell(scalerRuleData2, probe.getRef());

        // kill job to trigger completeLatch
        jobActor.tell(
            new JobClusterProto.KillJobRequest(
                jobId,"killed", JobCompletedReason.Killed, "test", probe.getRef()),
            probe.getRef());
        probe.expectMsgClass(JobClusterProto.KillJobResponse.class);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(completeLatch.await(5, TimeUnit.SECONDS));
        assertEquals(totalRuleChanges, ruleInfoChangesList.size());
        assertNull(ruleInfoChangesList.get(0).getRules());
        assertEquals(scalerRuleData1.getProtoRules(), ruleInfoChangesList.get(1).getRules());
        assertEquals(scalerRuleData2.getProtoRules(), ruleInfoChangesList.get(2).getRules());
    }
}
