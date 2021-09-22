/*
 * Copyright 2020 Netflix, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SourceJobParameters;
import io.mantisrx.server.core.NamedJobInfo;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import io.mantisrx.shaded.com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import rx.Observable;

public class SourceJobWorkerMetricsSubscriptionTest {
    @Test
    public void testGetSourceJobToClientMap() {
        List<SourceJobParameters.TargetInfo> infos = ImmutableList.of(
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("jobA").withQuery("criterion").withClientId("client1").build(),
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("jobA").withQuery("criterion").withClientId("client2").build(),
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("jobB").withQuery("criterion").withClientId("client1").build(),
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("jobB").withQuery("criterion").withClientId("client3").build()
        );

        SourceJobWorkerMetricsSubscription sub = new SourceJobWorkerMetricsSubscription(infos, null, null, null);
        Map<String, Set<String>> results = sub.getSourceJobToClientMap();
        Map<String, Set<String>> expected = ImmutableMap.of("jobA", ImmutableSet.of("client1", "client2"),
                "jobB", ImmutableSet.of("client1", "client3"));
        assertEquals(expected, results);
    }

    @Test
    public void testGetResultsForAllSourceJobs() throws Exception {
        List<SourceJobParameters.TargetInfo> infos = ImmutableList.of(
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("jobA").withQuery("criterion").withClientId("client1").build(),
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("jobA").withQuery("criterion").withClientId("client2").build(),
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("jobB").withQuery("criterion").withClientId("client1").build(),
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("jobB").withQuery("criterion").withClientId("client3").build()
        );

        MantisMasterClientApi masterClient = mock(MantisMasterClientApi.class);
        SourceJobWorkerMetricsSubscription sub = spy(new SourceJobWorkerMetricsSubscription(infos, masterClient, null, new AutoScaleMetricsConfig()));

        when(masterClient.namedJobInfo("jobA")).thenReturn(Observable.just(new NamedJobInfo("jobA", "jobA-1")));
        when(masterClient.namedJobInfo("jobB")).thenReturn(Observable.just(new NamedJobInfo("jobA", "jobB-2")));
        doReturn(Observable.just(Observable.just(new MantisServerSentEvent("jobA-event")))).when(sub).getResultsForJobId(eq("jobA-1"), any());
        doReturn(Observable.just(Observable.just(new MantisServerSentEvent("jobB-event")))).when(sub).getResultsForJobId(eq("jobB-2"), any());

        CountDownLatch latch = new CountDownLatch(2);
        Observable.merge(sub.getResults()).doOnNext(event -> {
           if ("jobA-event".equals(event.getEventAsString()) || "jobB-event".equals(event.getEventAsString())) {
               latch.countDown();
           }
        }).subscribe();
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());

        Set<String> jobAMetrics = ImmutableSet.of("PushServerSse:clientId=client1:*", "PushServerSse:clientId=client2:*",
                "ServerSentEventRequestHandler:clientId=client1:*", "ServerSentEventRequestHandler:clientId=client2:*");
        verify(sub, times(1)).getResultsForJobId("jobA-1", jobAMetrics);

        jobAMetrics = ImmutableSet.of("PushServerSse:clientId=client1:*", "PushServerSse:clientId=client3:*",
                "ServerSentEventRequestHandler:clientId=client1:*", "ServerSentEventRequestHandler:clientId=client3:*");
        verify(sub, times(1)).getResultsForJobId("jobB-2", jobAMetrics);
    }
}
