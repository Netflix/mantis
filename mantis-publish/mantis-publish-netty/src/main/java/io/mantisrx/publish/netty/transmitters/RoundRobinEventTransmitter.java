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

package io.mantisrx.publish.netty.transmitters;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import io.mantisrx.discovery.proto.JobDiscoveryInfo;
import io.mantisrx.discovery.proto.MantisWorker;
import io.mantisrx.publish.EventChannel;
import io.mantisrx.publish.EventTransmitter;
import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.config.MrePublishConfiguration;
import io.mantisrx.publish.internal.discovery.MantisJobDiscovery;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.mantisrx.publish.netty.pipeline.HttpEventChannel;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RoundRobinEventTransmitter implements EventTransmitter {

    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinEventTransmitter.class);

    private final MrePublishConfiguration configuration;
    private static volatile int nextWorkerIdx = 0;
    private final Registry registry;
    private final Timer channelSendTime;
    private final MantisJobDiscovery jobDiscovery;
    private final EventChannel eventChannel;
    private final Counter noWorkersDroppedCount;
    private final Counter noDiscoveryDroppedCount;

    public RoundRobinEventTransmitter(MrePublishConfiguration config,
                                      Registry registry,
                                      MantisJobDiscovery jobDiscovery,
                                      EventChannel eventChannel) {
        this.configuration = config;
        this.registry = registry;
        this.channelSendTime =
                SpectatorUtils.buildAndRegisterTimer(
                        registry, "sendTime", "channel", HttpEventChannel.CHANNEL_TYPE);

        this.noWorkersDroppedCount =
                SpectatorUtils.buildAndRegisterCounter(registry, "mantisEventsDropped", "reason", "noWorkers");
        this.noDiscoveryDroppedCount =
                SpectatorUtils.buildAndRegisterCounter(registry, "mantisEventsDropped", "reason", "noDiscoveryInfo");
        this.jobDiscovery = jobDiscovery;
        this.eventChannel = eventChannel;
    }

    @Override
    public void send(Event event, String stream) {
        String app = configuration.appName();
        String jobCluster = jobDiscovery.getJobCluster(app, stream);
        Optional<JobDiscoveryInfo> jobDiscoveryInfo = jobDiscovery.getCurrentJobWorkers(jobCluster);
        if (jobDiscoveryInfo.isPresent()) {
            List<MantisWorker> workers = jobDiscoveryInfo.get().getIngestStageWorkers().getWorkers();
            int numWorkers = workers.size();
            if (numWorkers > 0) {
                MantisWorker nextWorker = workers.get(Integer.remainderUnsigned(nextWorkerIdx++, numWorkers));
                final long start = registry.clock().wallTime();
                // TODO: propagate feedback - check for Retryable or NonRetryable Exception.
                Future<Void> future = eventChannel.send(nextWorker, event);
                final long end = registry.clock().wallTime();
                channelSendTime.record(end - start, TimeUnit.MILLISECONDS);
            } else {
                LOG.trace("No workers for job cluster {}, dropping event", jobCluster);
                noWorkersDroppedCount.increment();
            }
        } else {
            LOG.trace("No job discovery info for job cluster {}, dropping event", jobCluster);
            noDiscoveryDroppedCount.increment();
        }
    }
}
