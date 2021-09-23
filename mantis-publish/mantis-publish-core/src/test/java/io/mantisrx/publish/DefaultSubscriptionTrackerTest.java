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

package io.mantisrx.publish;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.mantis.discovery.proto.JobDiscoveryInfo;
import com.netflix.mantis.discovery.proto.MantisWorker;
import com.netflix.mantis.discovery.proto.StageWorkers;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.ipc.http.HttpClient;
import io.mantisrx.publish.api.StreamType;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.config.SampleArchaiusMrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.proto.MantisServerSubscription;
import io.mantisrx.publish.proto.MantisServerSubscriptionEnvelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;


public class DefaultSubscriptionTrackerTest {

    private static final int subscriptionExpiryIntervalSec = 1;
    private final Map<String, String> streamJobClusterMap = new HashMap<>();
    @Rule
    public WireMockRule mantisWorker1 = new WireMockRule(options().dynamicPort());
    @Rule
    public WireMockRule mantisWorker2 = new WireMockRule(options().dynamicPort());
    @Rule
    public WireMockRule mantisWorker3 = new WireMockRule(options().dynamicPort());
    private DefaultSubscriptionTracker subscriptionTracker;
    private MantisJobDiscovery mockJobDiscovery;
    private StreamManager mockStreamManager;
    private HttpClient httpClient;

    public DefaultSubscriptionTrackerTest() {
        streamJobClusterMap.put(StreamType.DEFAULT_EVENT_STREAM, "RequestEventSubTrackerTestJobCluster");
        streamJobClusterMap.put(StreamType.LOG_EVENT_STREAM, "LogEventSubTrackerTestJobCluster");
    }

    private MrePublishConfiguration testConfig() {
        DefaultSettableConfig settableConfig = new DefaultSettableConfig();
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.PUBLISH_JOB_CLUSTER_PROP_PREFIX + StreamType.DEFAULT_EVENT_STREAM, streamJobClusterMap.get(StreamType.DEFAULT_EVENT_STREAM));
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.PUBLISH_JOB_CLUSTER_PROP_PREFIX + StreamType.LOG_EVENT_STREAM, streamJobClusterMap.get(StreamType.LOG_EVENT_STREAM));
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.SUBS_REFRESH_INTERVAL_SEC_PROP, 30);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.SUBS_EXPIRY_INTERVAL_SEC_PROP, subscriptionExpiryIntervalSec);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.JOB_DISCOVERY_REFRESH_INTERVAL_SEC_PROP, 30);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.DISCOVERY_API_HOSTNAME_PROP, "127.0.0.1");
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.DISCOVERY_API_PORT_PROP, 7171);
        settableConfig.setProperty(SampleArchaiusMrePublishConfiguration.SUBS_FETCH_QUERY_PARAMS_STR_PROP, "app=DefaultSubscriptionTrackerTest&type=unit_test");

        PropertyRepository propertyRepository = DefaultPropertyFactory.from(settableConfig);
        return new SampleArchaiusMrePublishConfiguration(propertyRepository);
    }

    @BeforeEach
    public void setup() {
        MrePublishConfiguration mrePublishConfiguration = testConfig();
        Registry registry = new DefaultRegistry();
        mockJobDiscovery = mock(MantisJobDiscovery.class);
        mockStreamManager = spy(new StreamManager(registry, mrePublishConfiguration));
        mantisWorker1.start();
        mantisWorker2.start();
        mantisWorker3.start();
        httpClient = HttpClient.create(registry);
        subscriptionTracker = new DefaultSubscriptionTracker(mrePublishConfiguration, registry, mockJobDiscovery, mockStreamManager, httpClient);

        when(mockJobDiscovery.getStreamNameToJobClusterMapping(anyString())).thenReturn(streamJobClusterMap);
    }

    @AfterEach
    public void teardown() {
        mantisWorker1.shutdown();
        mantisWorker2.shutdown();
        mantisWorker3.shutdown();
    }

    @Test
    public void testSubscriptionsResolveToMajorityAmongWorkers() throws IOException {
        String streamName = StreamType.DEFAULT_EVENT_STREAM;
        String jobCluster = streamJobClusterMap.get(streamName);
        String jobId = jobCluster + "-1";

        Set<String> streams = Collections.singleton(streamName);
        when(mockStreamManager.getRegisteredStreams()).thenReturn(streams);

        // worker 1 subs list
        MantisServerSubscriptionEnvelope w1Subs = SubscriptionsHelper.createSubsEnvelope(2, 0);
        mantisWorker1.stubFor(get(urlMatching("/\\?jobId=.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(w1Subs)))
        );

        // workers 2 and 3 publish the same subs list
        MantisServerSubscriptionEnvelope majoritySubs = SubscriptionsHelper.createSubsEnvelope(5, 2);
        mantisWorker2.stubFor(get(urlMatching("/\\?jobId=.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(majoritySubs)))
        );
        mantisWorker3.stubFor(get(urlMatching("/\\?jobId=.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(majoritySubs)))
        );

        JobDiscoveryInfo jdi = new JobDiscoveryInfo(jobCluster, jobId,
                Collections.singletonMap(1, new StageWorkers(jobCluster, jobId, 1, Arrays.asList(
                        new MantisWorker("127.0.0.1", mantisWorker1.port()),
                        new MantisWorker("127.0.0.1", mantisWorker2.port()),
                        new MantisWorker("127.0.0.1", mantisWorker3.port())
                ))));
        when(mockJobDiscovery.getCurrentJobWorkers(jobCluster)).thenReturn(Optional.of(jdi));
        for (String stream : streamJobClusterMap.keySet()) {
            if (!stream.equals(StreamType.DEFAULT_EVENT_STREAM)) {
                when(mockJobDiscovery.getCurrentJobWorkers(streamJobClusterMap.get(streamJobClusterMap.get(stream)))).thenReturn(Optional.empty());
            }
        }
        subscriptionTracker.refreshSubscriptions();

        Set<String> currentSubIds = subscriptionTracker.getCurrentSubIds(streamName);
        // subs resolved to majority among workers
        assertEquals(majoritySubs.getSubscriptions().stream().map(MantisServerSubscription::getSubscriptionId).collect(Collectors.toSet()), currentSubIds);
        assertNotEquals(w1Subs.getSubscriptions().stream().map(MantisServerSubscription::getSubscriptionId).collect(Collectors.toSet()), currentSubIds);

        // verify all new subscriptions propagated as ADD to StreamManager
        ArgumentCaptor<Subscription> captor = ArgumentCaptor.forClass(Subscription.class);
        verify(mockStreamManager, times(5)).addStreamSubscription(captor.capture());
        List<Subscription> subsAdded = captor.getAllValues();
        Map<String, Subscription> subIdToSubMap = subsAdded.stream().collect(Collectors.toMap(Subscription::getSubscriptionId, s -> s));
        assertEquals(majoritySubs.getSubscriptionList().size(), subIdToSubMap.size());
        majoritySubs.getSubscriptionList().forEach(sub -> {
            assertTrue(subIdToSubMap.containsKey(sub.getSubscriptionId()));
            assertEquals(sub.getQuery(), subIdToSubMap.get(sub.getSubscriptionId()).getRawQuery());
        });

    }

    @Test
    public void testSubscriptionsFetchFailureHandling() throws IOException, InterruptedException {
        String streamName = StreamType.DEFAULT_EVENT_STREAM;
        String jobCluster = streamJobClusterMap.get(streamName);
        String jobId = jobCluster + "-1";

        Set<String> streams = Collections.singleton(streamName);
        when(mockStreamManager.getRegisteredStreams()).thenReturn(streams);

        // worker 1 subs list
        MantisServerSubscriptionEnvelope majoritySubs = SubscriptionsHelper.createSubsEnvelope(2, 0);
        mantisWorker1.stubFor(get(urlMatching("/\\?jobId=.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(majoritySubs)))
        );

        JobDiscoveryInfo jdi = new JobDiscoveryInfo(jobCluster, jobId,
                Collections.singletonMap(1, new StageWorkers(jobCluster, jobId, 1, Collections.singletonList(
                        new MantisWorker("127.0.0.1", mantisWorker1.port())
                ))));
        when(mockJobDiscovery.getCurrentJobWorkers(jobCluster)).thenReturn(Optional.of(jdi));
        for (String stream : streamJobClusterMap.keySet()) {
            if (!stream.equals(StreamType.DEFAULT_EVENT_STREAM)) {
                when(mockJobDiscovery.getCurrentJobWorkers(streamJobClusterMap.get(streamJobClusterMap.get(stream)))).thenReturn(Optional.empty());
            }
        }
        subscriptionTracker.refreshSubscriptions();

        Set<String> currentSubIds = subscriptionTracker.getCurrentSubIds(streamName);
        // subs resolved to majority among workers
        assertEquals(majoritySubs.getSubscriptions().stream().map(MantisServerSubscription::getSubscriptionId).collect(Collectors.toSet()), currentSubIds);

        // verify all new subscriptions propagated as ADD to StreamManager
        ArgumentCaptor<Subscription> captor = ArgumentCaptor.forClass(Subscription.class);
        verify(mockStreamManager, times(2)).addStreamSubscription(captor.capture());
        List<Subscription> subsAdded = captor.getAllValues();
        Map<String, Subscription> subIdToSubMap = subsAdded.stream().collect(Collectors.toMap(Subscription::getSubscriptionId, s -> s));
        assertEquals(majoritySubs.getSubscriptionList().size(), subIdToSubMap.size());
        majoritySubs.getSubscriptionList().forEach(sub -> {
            assertTrue(subIdToSubMap.containsKey(sub.getSubscriptionId()));
            assertEquals(sub.getQuery(), subIdToSubMap.get(sub.getSubscriptionId()).getRawQuery());
        });

        // simulate a subscription fetch failure on next refresh
        mantisWorker1.stubFor(get(urlMatching("/\\?jobId=.*"))
                .willReturn(aResponse()
                        .withStatus(400)
                ));

        subscriptionTracker.refreshSubscriptions();

        Set<String> currentSubIds2 = subscriptionTracker.getCurrentSubIds(streamName);
        // subs resolved to majority among workers
        assertEquals(majoritySubs.getSubscriptions().stream().map(MantisServerSubscription::getSubscriptionId).collect(Collectors.toSet()), currentSubIds2);

        Thread.sleep(subscriptionExpiryIntervalSec * 1000 + 100);

        subscriptionTracker.refreshSubscriptions();

        assertTrue(subscriptionTracker.getCurrentSubIds(streamName).isEmpty());

        // verify all previously added subscriptions cleaned up and propagated as REMOVE to StreamManager
        ArgumentCaptor<String> captor2 = ArgumentCaptor.forClass(String.class);
        verify(mockStreamManager, times(2)).removeStreamSubscription(captor2.capture());
        List<String> subsAdded2 = captor2.getAllValues();
        assertEquals(majoritySubs.getSubscriptionList().size(), subsAdded2.size());
        majoritySubs.getSubscriptionList().forEach(sub -> {
            assertTrue(subsAdded2.contains(sub.getSubscriptionId()));
        });
    }

    @Test
    public void testJobDiscoveryFailureHandling() throws IOException, InterruptedException {
        String streamName = StreamType.DEFAULT_EVENT_STREAM;
        String jobCluster = streamJobClusterMap.get(streamName);
        String jobId = jobCluster + "-1";

        Set<String> streams = Collections.singleton(streamName);
        when(mockStreamManager.getRegisteredStreams()).thenReturn(streams);

        // worker 1 subs list
        MantisServerSubscriptionEnvelope majoritySubs = SubscriptionsHelper.createSubsEnvelope(2, 0);
        mantisWorker1.stubFor(get(urlMatching("/\\?jobId=.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(majoritySubs)))
        );

        JobDiscoveryInfo jdi = new JobDiscoveryInfo(jobCluster, jobId,
                Collections.singletonMap(1, new StageWorkers(jobCluster, jobId, 1, Collections.singletonList(
                        new MantisWorker("127.0.0.1", mantisWorker1.port())
                ))));
        when(mockJobDiscovery.getCurrentJobWorkers(jobCluster)).thenReturn(Optional.of(jdi));
        for (String stream : streamJobClusterMap.keySet()) {
            if (!stream.equals(StreamType.DEFAULT_EVENT_STREAM)) {
                when(mockJobDiscovery.getCurrentJobWorkers(streamJobClusterMap.get(streamJobClusterMap.get(stream)))).thenReturn(Optional.empty());
            }
        }


        subscriptionTracker.refreshSubscriptions();

        Set<String> currentSubIds = subscriptionTracker.getCurrentSubIds(streamName);
        // subs resolved to majority among workers
        assertEquals(majoritySubs.getSubscriptions().stream().map(MantisServerSubscription::getSubscriptionId).collect(Collectors.toSet()), currentSubIds);

        // verify all new subscriptions propagated as ADD to StreamManager
        ArgumentCaptor<Subscription> captor = ArgumentCaptor.forClass(Subscription.class);
        verify(mockStreamManager, times(2)).addStreamSubscription(captor.capture());
        List<Subscription> subsAdded = captor.getAllValues();
        Map<String, Subscription> subIdToSubMap = subsAdded.stream().collect(Collectors.toMap(Subscription::getSubscriptionId, s -> s));
        assertEquals(majoritySubs.getSubscriptionList().size(), subIdToSubMap.size());
        majoritySubs.getSubscriptionList().forEach(sub -> {
            assertTrue(subIdToSubMap.containsKey(sub.getSubscriptionId()));
            assertEquals(sub.getQuery(), subIdToSubMap.get(sub.getSubscriptionId()).getRawQuery());
        });


        // simulate the job discovery fetch failure
        when(mockJobDiscovery.getCurrentJobWorkers(jobCluster)).thenReturn(Optional.empty());

        subscriptionTracker.refreshSubscriptions();

        Set<String> currentSubIds2 = subscriptionTracker.getCurrentSubIds(streamName);
        // subs resolved to majority among workers
        assertEquals(majoritySubs.getSubscriptions().stream().map(MantisServerSubscription::getSubscriptionId).collect(Collectors.toSet()), currentSubIds2);

        // sleep for subs expiry interval and refreshSubs to trigger a cleanup of subs due to job discovery failure
        Thread.sleep(subscriptionExpiryIntervalSec * 1000 + 100);
        subscriptionTracker.refreshSubscriptions();

        assertTrue(subscriptionTracker.getCurrentSubIds(streamName).isEmpty());

        // verify all previously added subscriptions cleaned up and propagated as REMOVE to StreamManager
        ArgumentCaptor<String> captor2 = ArgumentCaptor.forClass(String.class);
        verify(mockStreamManager, times(2)).removeStreamSubscription(captor2.capture());
        List<String> subsAdded2 = captor2.getAllValues();
        assertEquals(majoritySubs.getSubscriptionList().size(), subsAdded2.size());
        majoritySubs.getSubscriptionList().forEach(sub -> {
            assertTrue(subsAdded2.contains(sub.getSubscriptionId()));
        });
    }

    @Test
    public void testSubsNotRefreshOnNoRegisteredStreams() throws IOException {
        String streamName = StreamType.DEFAULT_EVENT_STREAM;
        String jobCluster = streamJobClusterMap.get(streamName);
        String jobId = jobCluster + "-1";

        Set<String> streams = Collections.singleton(streamName);
        when(mockStreamManager.getRegisteredStreams()).thenReturn(Collections.emptySet(), streams);
//        when(mockStreamManager.getRegisteredStreams()).thenReturn(streams);

        // worker 1 subs list
        MantisServerSubscriptionEnvelope majoritySubs = SubscriptionsHelper.createSubsEnvelope(2, 0);
        mantisWorker1.stubFor(get(urlMatching("/\\?jobId=.*"))
                                  .willReturn(aResponse()
                                                  .withStatus(200)
                                                  .withBody(DefaultObjectMapper.getInstance().writeValueAsBytes(majoritySubs)))
        );

        JobDiscoveryInfo jdi = new JobDiscoveryInfo(jobCluster, jobId,
                                                    Collections.singletonMap(1, new StageWorkers(jobCluster, jobId, 1, Collections.singletonList(
                                                        new MantisWorker("127.0.0.1", mantisWorker1.port())
                                                    ))));
        when(mockJobDiscovery.getCurrentJobWorkers(jobCluster)).thenReturn(Optional.of(jdi));
        for (String stream : streamJobClusterMap.keySet()) {
            if (!stream.equals(StreamType.DEFAULT_EVENT_STREAM)) {
                when(mockJobDiscovery.getCurrentJobWorkers(streamJobClusterMap.get(streamJobClusterMap.get(stream)))).thenReturn(Optional.empty());
            }
        }

        subscriptionTracker.refreshSubscriptions();

        assertTrue(subscriptionTracker.getCurrentSubIds(streamName).isEmpty());
        verify(mockStreamManager, times(1)).getRegisteredStreams();
        verifyZeroInteractions(mockJobDiscovery);
    }

    private static class SubscriptionsHelper {

        static MantisServerSubscriptionEnvelope createSubsEnvelope(int numSubs, int offset) {
            List<MantisServerSubscription> subList = new ArrayList<>(numSubs);
            for (int i = 0; i < numSubs; i++) {
                String subId = "testSubId-" + (i + offset);
                String query = "SELECT * FROM stream";
                subList.add(new MantisServerSubscription(subId, query));
            }
            return new MantisServerSubscriptionEnvelope(subList);
        }
    }
}
