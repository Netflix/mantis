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

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.discovery.proto.StreamJobClusterMap;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.core.Subscription;
import io.mantisrx.publish.core.SubscriptionFactory;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.proto.MantisServerSubscription;
import io.mantisrx.publish.proto.MantisServerSubscriptionEnvelope;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class handles the logic for stream -> job cluster discovery. A class that extends this abstract class is
 * expected to perform the actual fetch given a stream and cluster names. The storage of the subscriptions is off loaded
 * to the stream manager (See {@link StreamManager}). Thus, the stream manager is the source of truth of current active
 * subscriptions. For the same reason, the stream manager also stores which streams are registered. This class will only
 * fetch subscriptions for streams that are registered.
 */
public abstract class AbstractSubscriptionTracker implements SubscriptionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSubscriptionTracker.class);

    private final MrePublishConfiguration mrePublishConfiguration;
    private final Registry registry;
    private final MantisJobDiscovery jobDiscovery;
    private final StreamManager streamManager;
    private final Counter refreshSubscriptionInvokedCount;
    private final Counter refreshSubscriptionSuccessCount;
    private final Counter refreshSubscriptionFailedCount;
    private final Counter staleSubscriptionRemovedCount;

    public AbstractSubscriptionTracker(MrePublishConfiguration mrePublishConfiguration,
                                       Registry registry,
                                       MantisJobDiscovery jobDiscovery,
                                       StreamManager streamManager) {
        this.mrePublishConfiguration = mrePublishConfiguration;
        this.registry = registry;
        this.jobDiscovery = jobDiscovery;
        this.streamManager = streamManager;
        this.refreshSubscriptionInvokedCount = SpectatorUtils.buildAndRegisterCounter(registry,
                "refreshSubscriptionInvokedCount");
        this.refreshSubscriptionSuccessCount = SpectatorUtils.buildAndRegisterCounter(registry,
                "refreshSubscriptionSuccessCount");
        this.refreshSubscriptionFailedCount = SpectatorUtils.buildAndRegisterCounter(registry,
                "refreshSubscriptionFailedCount");
        this.staleSubscriptionRemovedCount = SpectatorUtils.buildAndRegisterCounter(registry,
                "staleSubscriptionRemovedCount");
    }

	/**
	 * Given a set of subscriptions representing the current universe of valid subscriptions
	 * this function propogates the changes by adding subscriptions that are not currently
	 * present and removing those that are no longer present.
	 *
	 * @param currentSubscriptions A {@link Set} of {@link MantisServerSubscription} representing all current subscriptions to be added or kept if present.
	 * @param extension A {@link Set} of {@link MantisServerSubscription} representing a subscriptions to be kept if present but not added if not present.
	 * */
	void propagateSubscriptionChanges(Set<MantisServerSubscription> currentSubscriptions, Set<MantisServerSubscription> extension) {
		Set<Subscription> previousSubscriptions = getCurrentSubscriptions();

		// Add newSubscriptions not present in previousSubscriptions
		currentSubscriptions.stream()
			.filter(c -> !previousSubscriptions.stream()
					.map(ps -> ps.getSubscriptionId())
					.collect(Collectors.toSet())
					.contains(c.getSubscriptionId()))
			.forEach(newSub -> {
				try {
					Optional<Subscription> subscription = SubscriptionFactory
						.getSubscription(newSub.getSubscriptionId(), newSub.getQuery());
					if (subscription.isPresent()) {
						streamManager.addStreamSubscription(subscription.get());
					} else {
						LOG.info("will not add invalid subscription {}", newSub);
					}
				} catch (Throwable t) {
					LOG.debug("failed to add subscription {}", newSub, t);
				}
			});

		Set<String> idsToKeep = currentSubscriptions.stream().map(x -> x.getSubscriptionId()).collect(Collectors.toSet());
		idsToKeep.addAll(extension.stream().map(x -> x.getSubscriptionId()).collect(Collectors.toSet()));

		// Remove previousSubscriptions not present in currentSubscriptions and not present
		// in the extension list
		previousSubscriptions.stream()
			.filter(o -> !idsToKeep.contains(o.getSubscriptionId()))
			.forEach(o -> {
				try {
					streamManager.removeStreamSubscription(o.getSubscriptionId());
				} catch (Throwable t) {
					LOG.debug("failed to remove subscription {}", o.getSubscriptionId());
				}
			});
	}

	/**
	 * Get current set of subscriptions for a given jobCluster.
	 *
	 * @param jobCluster Mantis Job Cluster name
	 *
	 * @return Optional of MantisServerSubscriptionEnvelope on successful retrieval, else empty
	 */
	public abstract Optional<MantisServerSubscriptionEnvelope> fetchSubscriptions(String jobCluster);


	/**
	 * Determines which job clusters (source jobs) are currently mapped to the
	 * specified application.
	 *
	 * @param streamJobClusterMap A {@link Map} of stream name to job cluster.
	 * @param registeredStreams A {@link Set} of registered streams.
	 *
	 * @return A {@link Set} of job clusters relevant to this application.
	 * */
	private Set<String> getRelevantJobClusters(Map<String, String> streamJobClusterMap, Set<String> registeredStreams) {
		Set<String> jobClustersToFetch = new HashSet<>();

		for (Map.Entry<String, String> e : streamJobClusterMap.entrySet()) {
			String streamName = e.getKey();
			LOG.debug("processing stream {} and currently registered Streams {}", streamName, registeredStreams);
			if (registeredStreams.contains(streamName)
					|| StreamJobClusterMap.DEFAULT_STREAM_KEY.equals(streamName)) {
				jobClustersToFetch.add(e.getValue());
			} else {
				LOG.debug("No server side mappings found for one or more streams {} ", registeredStreams);
				LOG.debug("will not fetch subscriptions for un-registered stream {}", streamName);
			}
		}

		return jobClustersToFetch;
	}



	private ConcurrentHashMap<String, SubscriptionCacheEntry> subsciptionCache = new ConcurrentHashMap<>();
	private class SubscriptionCacheEntry {
		public final long timestamp;
		public final String sourceJob;
		public final MantisServerSubscription sub;

		public SubscriptionCacheEntry(long timestamp, String sourceJob, MantisServerSubscription sub) {
			this.timestamp = timestamp;
			this.sourceJob = sourceJob;
			this.sub = sub;
		}
	}

	@Override
	public void refreshSubscriptions() {
		refreshSubscriptionInvokedCount.increment();

		boolean mantisPublishEnabled = mrePublishConfiguration.isMREClientEnabled();
		final Set<String> registeredStreams = streamManager.getRegisteredStreams();

		if (mantisPublishEnabled && !registeredStreams.isEmpty()) {
			final Map<String, String> streamJobClusterMap =
				jobDiscovery.getStreamNameToJobClusterMapping(mrePublishConfiguration.appName());
			Set<String> jobClustersToFetch = getRelevantJobClusters(streamJobClusterMap, registeredStreams);

			Set<MantisServerSubscription> allSubscriptions = new HashSet<>();
			Set<String> failedJobClusters = new HashSet<>();
			final long currentTimestamp = System.currentTimeMillis();

			for (String jobCluster : jobClustersToFetch) {
				try {
					Optional<MantisServerSubscriptionEnvelope> subsEnvelopeO = fetchSubscriptions(jobCluster);
					if (subsEnvelopeO.isPresent()) {
						MantisServerSubscriptionEnvelope subsEnvelope = subsEnvelopeO.get();

						for (MantisServerSubscription sub : subsEnvelope.getSubscriptions()) {
							subsciptionCache.put(sub.getSubscriptionId(), new SubscriptionCacheEntry(currentTimestamp, jobCluster, sub));
						}

						allSubscriptions.addAll(subsEnvelope.getSubscriptions());
						refreshSubscriptionSuccessCount.increment();
					} else {
						failedJobClusters.add(jobCluster);
						refreshSubscriptionFailedCount.increment();
					}
				} catch (Exception ex) {
					failedJobClusters.add(jobCluster);
					LOG.info("refresh subscriptions failed for {}", jobCluster, ex);
					refreshSubscriptionFailedCount.increment();
				}
			}

			Set<MantisServerSubscription> subscriptionsToExtend = subsciptionCache.entrySet().stream()
				.filter(es -> failedJobClusters.contains(es.getValue().sourceJob))
				.filter(es -> currentTimestamp - es.getValue().timestamp < mrePublishConfiguration.subscriptionExpiryIntervalSec() * 1000)
				.map(es -> es.getValue().sub)
				.collect(Collectors.toSet());

			propagateSubscriptionChanges(allSubscriptions, subscriptionsToExtend);

			// Cache Eviction
			subsciptionCache.entrySet().stream()
				.filter(es -> currentTimestamp - es.getValue().timestamp > mrePublishConfiguration.subscriptionExpiryIntervalSec() * 1000 * 10)
				.forEach(es -> subsciptionCache.remove(es.getKey()));

		} else {
			LOG.debug("subscription refresh skipped (client enabled {} registered streams {})",
					mantisPublishEnabled, registeredStreams);
		}
	}

	protected Set<Subscription> getCurrentSubscriptions() {

		return streamManager
			.getRegisteredStreams()
			.stream()
			.flatMap(streamName -> streamManager.getStreamSubscriptions(streamName).stream())
			.collect(Collectors.toSet());
	}
}
