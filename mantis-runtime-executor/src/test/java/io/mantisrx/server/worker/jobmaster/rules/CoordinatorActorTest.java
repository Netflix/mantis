package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.common.JsonSerializer;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.descriptor.StageScalingPolicy;
import io.mantisrx.runtime.descriptor.StageSchedulingInfo;
import io.mantisrx.server.core.JobScalerRuleInfo;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.jobmaster.JobAutoScalerService;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import rx.subjects.BehaviorSubject;


import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Slf4j
public class CoordinatorActorTest {
    private static final String JOB_ID = "test-job-id";
    private static final String RULE_ID_1 = "1";
    private static final String RULE_ID_2 = "2";
    private static final Duration Max_Duration = Duration.of(5000, ChronoUnit.MILLIS);
    private static final Duration Interval_Duration = Duration.of(500, ChronoUnit.MILLIS);

    private ActorSystem system;
    private TestKit testKit;

    private JobScalerContext jobScalerContext;
    private StageScalingPolicy defaultStageScalingPolicy;

    @Mock
    private MantisMasterGateway masterClientApi;

    @Mock
    private JobAutoScalerService jobAutoScalerService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        system = ActorSystem.create();
        testKit = new TestKit(system);
        defaultStageScalingPolicy = TestRuleUtils.createDefaultStageScalingPolicy(1);
        jobScalerContext = JobScalerContext.builder()
            .jobId(JOB_ID)
            .masterClientApi(masterClientApi)
            .jobAutoScalerServiceFactory((context, rule) -> jobAutoScalerService)
            .schedInfo(new SchedulingInfo.Builder()
                .addStage(StageSchedulingInfo.builder()
                    .scalingPolicy(defaultStageScalingPolicy).build())
                .numberOfStages(1)
                .build())
            .build();

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
    public void testOnRuleRefreshRuleNoDefault() throws InterruptedException {
        // override default
        this.jobScalerContext = JobScalerContext.builder()
            .jobId(JOB_ID)
            .masterClientApi(masterClientApi)
            .jobAutoScalerServiceFactory((context, rule) -> jobAutoScalerService)
            .build();

        JobScalingRule perpetualRule = TestRuleUtils.createPerpetualRule(RULE_ID_1, JOB_ID);
        JsonSerializer serializer = new JsonSerializer();
        try {
            String jsonStr = serializer.toJson(perpetualRule);
            log.info("Test: perpetual rule json: \n{}", jsonStr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        JobScalerRuleInfo ruleInfo = new JobScalerRuleInfo(
            JOB_ID, false, Collections.singletonList(perpetualRule));

        JobScalerRuleInfo ruleInfo2 = new JobScalerRuleInfo(JOB_ID, false,
            Collections.emptyList());

        BehaviorSubject<JobScalerRuleInfo> ruleInfoSubject = BehaviorSubject.create();
        ruleInfoSubject.onNext(ruleInfo2);
        when(masterClientApi.jobScalerRulesStream(anyString()))
            .thenReturn(ruleInfoSubject);

        ActorRef coordinatorActor = system.actorOf(CoordinatorActor.Props(jobScalerContext), "coordinatorActor");
        log.info("Test: create coordinator actor: {}", coordinatorActor);
        final TestKit probe = new TestKit(system);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);
                assertNotNull(state);
                assertEquals(0, state.getCurrentRuleInfo().getRules().size());
                checkActiveControllerRule(state, probe, null);
                return null;
        });

        // add a rule
        ruleInfoSubject.onNext(ruleInfo);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state2 = getState(coordinatorActor, probe);
                assertNotNull(state2);
                assertEquals(ruleInfo.getRules(), state2.getCurrentRuleInfo().getRules());

                // check active controller rule state again
                checkActiveControllerRule(state2, probe, perpetualRule);
                return null;
            });

        // reset again
        ruleInfoSubject.onNext(ruleInfo2);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state3 = getState(coordinatorActor, probe);
                assertNotNull(state3);
                assertEquals(0, state3.getCurrentRuleInfo().getRules().size());

                // check active controller rule state again
                checkActiveControllerRule(state3, probe, null);
                return null;
            });
    }

    @Test
    public void testOnRuleRefreshWithPerpetualRuleWithDefault() throws InterruptedException {
        JobScalingRule perpetualRule = TestRuleUtils.createPerpetualRule(RULE_ID_1, JOB_ID);
        JobScalingRule perpetualRule2 = TestRuleUtils.createPerpetualRule(RULE_ID_2, JOB_ID);
        JobScalerRuleInfo ruleInfo = new JobScalerRuleInfo(
            JOB_ID, false, Collections.singletonList(perpetualRule));

        BehaviorSubject<JobScalerRuleInfo> ruleInfoSubject = BehaviorSubject.create();
        ruleInfoSubject.onNext(ruleInfo);
        when(masterClientApi.jobScalerRulesStream(anyString()))
            .thenReturn(ruleInfoSubject);
        JobScalerRuleInfo ruleInfo2 = new JobScalerRuleInfo(JOB_ID, false,
            Collections.emptyList());
        JobScalerRuleInfo ruleInfo3 = new JobScalerRuleInfo(JOB_ID, false,
            ImmutableList.of(perpetualRule,perpetualRule2));

        ActorRef coordinatorActor = system.actorOf(CoordinatorActor.Props(jobScalerContext), "coordinatorActor");
        log.info("Test: create coordinator actor: {}", coordinatorActor);
        final TestKit probe = new TestKit(system);

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);

                assertNotNull(state);
                assertEquals(ruleInfo.getRules(), state.getCurrentRuleInfo().getRules());

                // check active controller rule state
                checkActiveControllerRule(state, probe, ruleInfo.getRules().get(0));
                return null;
            });


        // push direct actor update
        coordinatorActor.tell(ruleInfo2, probe.getRef());
        CoordinatorActor.GetStateResponse state2 = getState(coordinatorActor, probe);
        assertNotNull(state2);
        assertEquals(ruleInfo2.getRules(), state2.getCurrentRuleInfo().getRules());

        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                // check active controller rule state again
                log.info("Test: check active controller rule state: expect default rule.");
                checkActiveControllerRule(state2, probe, state2.getDefaultRule());
                assertEquals(
                    1, state2.getDefaultRule().getScalerConfig().getScalingPolicies().size());
                assertEquals(
                    this.defaultStageScalingPolicy,
                    state2.getDefaultRule().getScalerConfig().getScalingPolicies().get(0));
                assertNull(state2.getDefaultRule().getTriggerConfig());

                assertEquals(0, state2.getDefaultRule().getScalerConfig().getStageDesireSize().size());
                return null;
            });

        // push update to rule stream
        ruleInfoSubject.onNext(ruleInfo3);
        testKit.awaitAssert(Max_Duration, Interval_Duration,
            () -> {
                CoordinatorActor.GetStateResponse state3 = getState(coordinatorActor, probe);
                assertNotNull(state3);
                assertEquals(ruleInfo3.getRules(), state3.getCurrentRuleInfo().getRules());

                checkActiveControllerRule(state3, probe, ruleInfo3.getRules().get(1));
                return null;
            });

    }

    private CoordinatorActor.GetStateResponse getState(ActorRef coordinatorActor, TestKit probe) {
        coordinatorActor.tell(CoordinatorActor.GetStateRequest.of(JOB_ID), probe.getRef());
        return probe.expectMsgClass(CoordinatorActor.GetStateResponse.class);
    }

    private void checkActiveControllerRule(
        CoordinatorActor.GetStateResponse state,
        TestKit probe,
        JobScalingRule expectedRule) {
        state.getControllerActor().tell(new ScalerControllerActor.GetActiveRuleRequest(), probe.getRef());
        ScalerControllerActor.GetActiveRuleResponse activeRuleResponse =
            probe.expectMsgClass(ScalerControllerActor.GetActiveRuleResponse.class);
        assertNotNull(activeRuleResponse);
        assertEquals(expectedRule, activeRuleResponse.getRule());
    }
}
