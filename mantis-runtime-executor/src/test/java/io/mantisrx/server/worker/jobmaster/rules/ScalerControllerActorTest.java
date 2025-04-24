package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.jobmaster.JobAutoScalerService;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.server.worker.jobmaster.akka.rules.CoordinatorActor;
import io.mantisrx.server.worker.jobmaster.akka.rules.ScalerControllerActor;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import rx.Observable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Slf4j
public class ScalerControllerActorTest {
    private static final String JOB_ID = "test-job-id";
    private static final String RULE_ID_1 = "1";
    private static final String RULE_ID_2 = "2";
    private static final Duration Max_Duration = Duration.of(5000, ChronoUnit.MILLIS);
    private static final Duration Interval_Duration = Duration.of(500, ChronoUnit.MILLIS);

    private ActorSystem system;
    private TestKit testKit;

    private JobScalerContext jobScalerContext;

    @Mock
    private MantisMasterGateway masterClientApi;

    @Mock
    private JobAutoScalerService jobAutoScalerService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        system = ActorSystem.create();
        testKit = new TestKit(system);
        jobScalerContext = JobScalerContext.builder()
            .jobId(JOB_ID)
            .masterClientApi(masterClientApi)
            .jobAutoScalerServiceFactory((context, rule) -> jobAutoScalerService)
            .schedInfo(new SchedulingInfo.Builder()
                .addStage(StageSchedulingInfo.builder()
                    .scalingPolicy(TestRuleUtils.createDefaultStageScalingPolicy(1)).build())
                .numberOfStages(1)
                .build())
            .build();

        when(masterClientApi.scaleJobStage(anyString(), anyInt(), anyInt(), anyString()))
            .thenReturn(Observable.just(true));

        doAnswer((Answer<Void>) invocation -> {
            log.info("Test: start job auto scaler service");
            return null;
        }).when(jobAutoScalerService).start();
        doAnswer((Answer<Void>) invocation -> {
            log.info("Test: shutdown job auto scaler service");
            return null;
        }).when(jobAutoScalerService).shutdown();
    }

    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
        testKit = null;
    }

    @Test
    public void testOnRuleRefreshWithPerpetualRuleWithDefault() {
        JobScalingRule perpetualRule = TestRuleUtils.createPerpetualRule(RULE_ID_1, JOB_ID);
        JobScalingRule perpetualRule2 = TestRuleUtils.createPerpetualRule(RULE_ID_2, JOB_ID);

        ActorRef controllerActor = system.actorOf(ScalerControllerActor.Props(jobScalerContext), "controllerActor");
        final TestKit probe = new TestKit(system);

        // test activate rule 1
        controllerActor.tell(CoordinatorActor.ActivateRuleRequest.of(JOB_ID, perpetualRule), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        ScalerControllerActor.GetActiveRuleResponse response =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule, response.getRule());

        // test activate rule 2
        controllerActor.tell(CoordinatorActor.ActivateRuleRequest.of(JOB_ID, perpetualRule2), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        response = probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule2, response.getRule());

        // test ignore low ranking rule
        controllerActor.tell(CoordinatorActor.ActivateRuleRequest.of(JOB_ID, perpetualRule), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        response = probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule2, response.getRule());

        // test ignore deactivate rule
        controllerActor.tell(CoordinatorActor.DeactivateRuleRequest.of(JOB_ID, perpetualRule.getRuleId()), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        response = probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule2, response.getRule());

        // test deactivate active rule
        controllerActor.tell(CoordinatorActor.DeactivateRuleRequest.of(JOB_ID, perpetualRule2.getRuleId()), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        response = probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertNull(response.getRule());

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                verify(jobAutoScalerService, times(2)).start();
                verify(jobAutoScalerService, times(2)).shutdown();
                return null;
            });
    }

    @Test
    public void testOnRuleRefreshWithDesireSize() {
        JobScalingRule perpetualRule = TestRuleUtils.createPerpetualRuleWithDesireSize(RULE_ID_1, JOB_ID);

        ActorRef controllerActor = system.actorOf(ScalerControllerActor.Props(jobScalerContext), "controllerActor");
        final TestKit probe = new TestKit(system);

        controllerActor.tell(CoordinatorActor.ActivateRuleRequest.of(JOB_ID, perpetualRule), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        ScalerControllerActor.GetActiveRuleResponse response =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule, response.getRule());

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                verify(jobAutoScalerService, times(1)).start();
                return null;
            });
    }

    @Test
    public void testOnRuleRefreshWithDesireSizeOnly() {
        JobScalingRule perpetualRule1 = TestRuleUtils.createPerpetualRuleWithDesireSize(RULE_ID_1, JOB_ID);
        JobScalingRule perpetualRule2 = TestRuleUtils.createPerpetualRuleWithDesireSizeOnly(RULE_ID_2, JOB_ID);

        ActorRef controllerActor = system.actorOf(ScalerControllerActor.Props(jobScalerContext), "controllerActor");
        final TestKit probe = new TestKit(system);

        controllerActor.tell(CoordinatorActor.ActivateRuleRequest.of(JOB_ID, perpetualRule1), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        ScalerControllerActor.GetActiveRuleResponse response =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule1, response.getRule());

        controllerActor.tell(CoordinatorActor.ActivateRuleRequest.of(JOB_ID, perpetualRule2), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        response =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule2, response.getRule());

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                // no service should be stared since no scaling policy is defined
                verify(jobAutoScalerService, times(1)).start();
                verify(jobAutoScalerService, times(1)).shutdown();
                return null;
            });
    }

    @Test
    public void testOnRuleRefreshFailedStart() {
        JobScalingRule perpetualRule = TestRuleUtils.createPerpetualRule(RULE_ID_1, JOB_ID);
        JobScalingRule perpetualRule2 = TestRuleUtils.createPerpetualRule(RULE_ID_2, JOB_ID);

        ActorRef controllerActor = system.actorOf(ScalerControllerActor.Props(jobScalerContext), "controllerActor");
        final TestKit probe = new TestKit(system);

        CountDownLatch latch1 = new CountDownLatch(1);
        AtomicInteger serviceNum = new AtomicInteger();
        doAnswer((Answer<Void>) invocation -> {
            if (serviceNum.get() == 0) {
                log.info("Test Block: job auto scaler service");
                serviceNum.getAndIncrement();
                assertTrue(latch1.await(10, TimeUnit.SECONDS));
                throw new RuntimeException("Mock start service failure");
            } else {
                log.info("Test: start job auto scaler service");
            }
            return null;
        }).when(jobAutoScalerService).start();

        // trigger rule 1 activation first but block the start service call
        controllerActor.tell(CoordinatorActor.ActivateRuleRequest.of(JOB_ID, perpetualRule), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        ScalerControllerActor.GetActiveRuleResponse response =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule, response.getRule());

        // trigger rule 2 activation
        controllerActor.tell(CoordinatorActor.ActivateRuleRequest.of(JOB_ID, perpetualRule2), probe.getRef());
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        response =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule2, response.getRule());

        // unblock the rule 1 activation
        latch1.countDown();

        // rule 2 should be active still (ignore failed rule 1 activation)
        controllerActor.tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        response =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertEquals(perpetualRule2, response.getRule());

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                verify(jobAutoScalerService, times(2)).start();
                return null;
            });
    }
}
