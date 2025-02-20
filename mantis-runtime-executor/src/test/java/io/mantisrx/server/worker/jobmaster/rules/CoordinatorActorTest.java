package io.mantisrx.server.worker.jobmaster.rules;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.runtime.descriptor.JobScalingRule;
import io.mantisrx.server.core.JobScalerRuleInfo;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.worker.jobmaster.JobScalerContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import rx.Observable;
import rx.subjects.BehaviorSubject;


import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Slf4j
public class CoordinatorActorTest {
    private static final String JOB_ID = "test-job-id";
    private static final String RULE_ID_1 = "1";
    private static final String RULE_ID_2 = "2";

    private ActorSystem system;
    private TestKit testKit;

    private JobScalerContext jobScalerContext;

    @Mock
    private MantisMasterGateway masterClientApi;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        system = ActorSystem.create();
        testKit = new TestKit(system);
        jobScalerContext = JobScalerContext.builder()
            .jobId(JOB_ID)
            .masterClientApi(masterClientApi)
            .build();

    }

    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
        testKit = null;
    }

    @Test
    public void testOnRuleRefreshWithPerpetualRuleNoDefault() throws InterruptedException {
        JobScalingRule perpetualRule = TestRuleUtils.createPerpetualRule(RULE_ID_1, JOB_ID);
        JobScalerRuleInfo ruleInfo = new JobScalerRuleInfo(JOB_ID, false, Collections.singletonList(perpetualRule));

        BehaviorSubject<JobScalerRuleInfo> ruleInfoSubject = BehaviorSubject.create();
        ruleInfoSubject.onNext(ruleInfo);
        when(masterClientApi.jobScalerRulesStream(anyString()))
            .thenReturn(ruleInfoSubject);
        JobScalerRuleInfo ruleInfo2 = new JobScalerRuleInfo(JOB_ID, false,
            Collections.emptyList());

        ActorRef coordinatorActor = system.actorOf(CoordinatorActor.Props(jobScalerContext), "coordinatorActor");
        log.info("Test: create coordinator actor: {}", coordinatorActor);
        final TestKit probe = new TestKit(system);
//        coordinatorActor.tell(CoordinatorActor.InitRequest.of(JOB_ID), probe.getRef());
//        CoordinatorActor.InitResponse initResponse = probe.expectMsgClass(CoordinatorActor.InitResponse.class);
//        assertEquals(JOB_ID, initResponse.getJobId());

        Thread.sleep(300);
        CoordinatorActor.GetStateResponse state = getState(coordinatorActor, probe);

        assertNotNull(state);
        assertEquals(ruleInfo.getRules(), state.getCurrentRuleInfo().getRules());

        // check active controller rule state
        checkActiveControllerRule(state, probe, ruleInfo.getRules().get(0));

        // push direct actor update
        coordinatorActor.tell(ruleInfo2, probe.getRef());

        CoordinatorActor.GetStateResponse state2 = getState(coordinatorActor, probe);

        assertNotNull(state2);
        assertEquals(ruleInfo2.getRules(), state2.getCurrentRuleInfo().getRules());

        // check active controller rule state again
        checkActiveControllerRule(state2, probe, null);

        // push update to rule stream
        ruleInfoSubject.onNext(ruleInfo);
        Thread.sleep(300);
        CoordinatorActor.GetStateResponse state3 = getState(coordinatorActor, probe);
        assertNotNull(state3);
        assertEquals(ruleInfo.getRules(), state3.getCurrentRuleInfo().getRules());

        checkActiveControllerRule(state3, probe, ruleInfo.getRules().get(0));
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
