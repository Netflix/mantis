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

package io.mantisrx.master.scheduler;

import static io.mantisrx.master.scheduler.AgentsErrorMonitorActor.props;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import io.mantisrx.master.events.LifecycleEventsProto;
import io.mantisrx.master.jobcluster.job.worker.WorkerState;
import io.mantisrx.server.core.domain.WorkerId;
import io.mantisrx.server.master.scheduler.MantisScheduler;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import rx.functions.Action1;

public class AgentsErrorMonitorTest {

    static ActorSystem system;
    private static TestKit probe;


    @BeforeAll
    public static void setup() {
        system = ActorSystem.create();
        probe = new TestKit(system);
    }

    @AfterAll
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void hostErrorTest_disableHost() {
        EnableHostAction enableHostAction = new EnableHostAction();
        AgentsErrorMonitorActor.HostErrors hostErrors = new AgentsErrorMonitorActor.HostErrors("host1", enableHostAction, 120000, 3);
        long t1 = 1000;
        LifecycleEventsProto.WorkerStatusEvent workerStatusEvent  = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        assertFalse(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));
        t1+=100;

        workerStatusEvent = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        assertFalse(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));
        t1+=100;

        workerStatusEvent  = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        assertFalse(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));
        t1+=100;

        assertEquals(0,enableHostAction.getEnableHostList().size());

        // no of errors is now 4 which is greater than 3
        workerStatusEvent = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        assertTrue(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));


    }

    @Test
    public void hostErrorTest_enableHost() {
        EnableHostAction enableHostAction = new EnableHostAction();
        AgentsErrorMonitorActor.HostErrors hostErrors = new AgentsErrorMonitorActor.HostErrors("host1", enableHostAction, 120000, 3);
        long t1 = 1000;
        LifecycleEventsProto.WorkerStatusEvent workerStatusEvent  = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        assertFalse(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));
        t1+=100;

        workerStatusEvent = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        assertFalse(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));
        t1+=100;

        workerStatusEvent  = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        assertFalse(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));
        t1+=100;

        assertEquals(0,enableHostAction.getEnableHostList().size());

        // no of errors is now 4 which is greater than 3
        workerStatusEvent = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        assertTrue(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));

        workerStatusEvent = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Started, t1, "host1");
        assertFalse(hostErrors.addAndGetIsTooManyErrors(workerStatusEvent));

        // 4th event comes in with a non terminal event. This should reenable the host
        assertEquals(1,  enableHostAction.getEnableHostList().size());
        assertEquals("host1", enableHostAction.getEnableHostList().get(0));


    }



    @Test
    public void basicTest() {

        long too_old_millis = 4000;
        int error_check_window_count = 3;
        long error_check_window_millis = 2000;
        long disableDuration = 1000;
        long t1 = 1000;
        ActorRef errorMonitorActor = system.actorOf(props(too_old_millis, error_check_window_count, error_check_window_millis,disableDuration));

        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        errorMonitorActor.tell(new AgentsErrorMonitorActor.InitializeAgentsErrorMonitor(schedulerMock), probe.getRef());

        LifecycleEventsProto.WorkerStatusEvent workerStatusEvent  = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());
        t1+=100;

        workerStatusEvent = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());
        t1+=100;

        workerStatusEvent  = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());
        t1+=100;

        errorMonitorActor.tell(new AgentsErrorMonitorActor.HostErrorMapRequest(), probe.getRef());
        AgentsErrorMonitorActor.HostErrorMapResponse hostErrorMapResponse = probe.expectMsgClass(AgentsErrorMonitorActor.HostErrorMapResponse.class);
        assertTrue(hostErrorMapResponse.getMap().containsKey("host1"));
        List<Long> errorTsList = hostErrorMapResponse.getMap().get("host1").getErrorTimestampList();
        assertEquals(3, errorTsList.size());



        workerStatusEvent = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());

        errorMonitorActor.tell(new AgentsErrorMonitorActor.HostErrorMapRequest(), probe.getRef());
        hostErrorMapResponse = probe.expectMsgClass(AgentsErrorMonitorActor.HostErrorMapResponse.class);
        assertTrue(hostErrorMapResponse.getMap().containsKey("host1"));
        errorTsList = hostErrorMapResponse.getMap().get("host1").getErrorTimestampList();
        assertEquals(4, errorTsList.size());


        verify(schedulerMock, times(1)).disableVM("host1",disableDuration);


    }

    @Test
    public void testOldHostEviction() {

        EnableHostAction enableHostAction = new EnableHostAction();
        DisableHostAction disableHostAction = new DisableHostAction();
        long too_old_millis = 4000;
        int error_check_window_count = 3;
        long error_check_window_millis = 2000;
        long t1 = 1000;
        ActorRef errorMonitorActor = system.actorOf(props(too_old_millis, error_check_window_count, error_check_window_millis, 1000));

        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        errorMonitorActor.tell(new AgentsErrorMonitorActor.InitializeAgentsErrorMonitor(schedulerMock), probe.getRef());

        LifecycleEventsProto.WorkerStatusEvent workerStatusEvent  = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());
        t1+=100;

        workerStatusEvent = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());
        t1+=100;

        workerStatusEvent  = generateWorkerEvent("sine-function-1-worker-0-4", WorkerState.Failed, t1, "host1");
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());
        t1+=100;

        // ensure host 1 is registered in error map
        errorMonitorActor.tell(new AgentsErrorMonitorActor.HostErrorMapRequest(), probe.getRef());
        AgentsErrorMonitorActor.HostErrorMapResponse hostErrorMapResponse = probe.expectMsgClass(AgentsErrorMonitorActor.HostErrorMapResponse.class);
        assertTrue(hostErrorMapResponse.getMap().containsKey("host1"));

        // simulate periodic check in the future
        errorMonitorActor.tell(new AgentsErrorMonitorActor.CheckHostHealthMessage(t1+ 100000), probe.getRef());

        // host1 should've been evicted as no new events were seen from it
        errorMonitorActor.tell(new AgentsErrorMonitorActor.HostErrorMapRequest(), probe.getRef());
        hostErrorMapResponse = probe.expectMsgClass(AgentsErrorMonitorActor.HostErrorMapResponse.class);
        assertTrue(hostErrorMapResponse.getMap().isEmpty());


    }

    @Test
    public void noHostEventIgnoredTest() {

        EnableHostAction enableHostAction = new EnableHostAction();
        DisableHostAction disableHostAction = new DisableHostAction();
        long too_old_millis = 4000;
        int error_check_window_count = 3;
        long error_check_window_millis = 2000;
        long t1 = 1000;
        ActorRef errorMonitorActor = system.actorOf(props( too_old_millis, error_check_window_count, error_check_window_millis, 1000));

        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        errorMonitorActor.tell(new AgentsErrorMonitorActor.InitializeAgentsErrorMonitor(schedulerMock), probe.getRef());

        LifecycleEventsProto.WorkerStatusEvent workerStatusEvent  = new LifecycleEventsProto.WorkerStatusEvent(
                LifecycleEventsProto.StatusEvent.StatusEventType.INFO,
                "test message",
                1,
                WorkerId.fromId("sine-function-1-worker-0-4").get(),
                WorkerState.Failed,

                1000);
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());

        errorMonitorActor.tell(new AgentsErrorMonitorActor.HostErrorMapRequest(), probe.getRef());
        AgentsErrorMonitorActor.HostErrorMapResponse hostErrorMapResponse = probe.expectMsgClass(AgentsErrorMonitorActor.HostErrorMapResponse.class);
        assertTrue(hostErrorMapResponse.getMap().isEmpty());



    }

    @Test
    public void eventFromWorkerNotYetOnHostIgnoredTest() {

        EnableHostAction enableHostAction = new EnableHostAction();
        DisableHostAction disableHostAction = new DisableHostAction();
        long too_old_millis = 4000;
        int error_check_window_count = 3;
        long error_check_window_millis = 2000;
        long t1 = 1000;
        ActorRef errorMonitorActor = system.actorOf(props(too_old_millis, error_check_window_count, error_check_window_millis, 1000));
        MantisScheduler schedulerMock = mock(MantisScheduler.class);
        errorMonitorActor.tell(new AgentsErrorMonitorActor.InitializeAgentsErrorMonitor(schedulerMock), probe.getRef());

        LifecycleEventsProto.WorkerStatusEvent workerStatusEvent  = new LifecycleEventsProto.WorkerStatusEvent(
                LifecycleEventsProto.StatusEvent.StatusEventType.INFO,
                "test message",
                1,
                WorkerId.fromId("sine-function-1-worker-0-4").get(),
                WorkerState.Launched,
                "host1",
                1000);
        errorMonitorActor.tell(workerStatusEvent,probe.getRef());

        errorMonitorActor.tell(new AgentsErrorMonitorActor.HostErrorMapRequest(), probe.getRef());
        AgentsErrorMonitorActor.HostErrorMapResponse hostErrorMapResponse = probe.expectMsgClass(AgentsErrorMonitorActor.HostErrorMapResponse.class);
        assertTrue(hostErrorMapResponse.getMap().isEmpty());



    }


    private LifecycleEventsProto.WorkerStatusEvent generateWorkerEvent(String id, WorkerState state, long ts, String host) {
        return new LifecycleEventsProto.WorkerStatusEvent(
                LifecycleEventsProto.StatusEvent.StatusEventType.INFO,
                "test message",
                1,
                WorkerId.fromId(id).get(),
                state,
                host,
                ts);
    }

    class EnableHostAction implements Action1<String>{
        List<String> enableHostList = new ArrayList<>();
        public EnableHostAction() {

        }

        @Override
        public void call(String s) {
            enableHostList.add(s);
        }

        public List<String> getEnableHostList() {
            return this.enableHostList;
        }
    }

    class DisableHostAction implements Action1<String>{
        List<String> disableHostList = new ArrayList<>();
        public DisableHostAction() {

        }

        @Override
        public void call(String s) {
            disableHostList.add(s);
        }

        public List<String> getDisableHostList() {
            return this.disableHostList;
        }
    }

}
