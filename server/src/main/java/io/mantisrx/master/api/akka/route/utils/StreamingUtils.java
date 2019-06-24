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

package io.mantisrx.master.api.akka.route.utils;

import akka.http.javadsl.model.sse.ServerSentEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.master.api.akka.route.proto.JobClusterInfo;
import io.mantisrx.server.core.JobSchedulingInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static io.mantisrx.master.api.akka.route.utils.JobDiscoveryHeartbeats.JOB_CLUSTER_INFO_HB_INSTANCE;
import static io.mantisrx.master.api.akka.route.utils.JobDiscoveryHeartbeats.SCHED_INFO_HB_INSTANCE;

public class StreamingUtils {
    private static final Logger logger = LoggerFactory.getLogger(StreamingUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper().configure(
            DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static volatile Optional<ServerSentEvent> cachedSchedInfoHbEvent = Optional.empty();
    private static volatile Optional<ServerSentEvent> cachedJobClusterInfoHbEvent = Optional.empty();


    static {
        try {
            cachedJobClusterInfoHbEvent = Optional.of(ServerSentEvent.create(mapper.writeValueAsString(
                    JOB_CLUSTER_INFO_HB_INSTANCE)));
            cachedSchedInfoHbEvent = Optional.of(ServerSentEvent.create(mapper.writeValueAsString(
                    SCHED_INFO_HB_INSTANCE)));
        } catch (JsonProcessingException e) {
            logger.error("Failed to cache serialized Heartbeat event", e);
        }
    }

    public static Optional<ServerSentEvent> from(final JobSchedulingInfo jsi) {
        try {
            if (jsi.getJobId().equals(JobSchedulingInfo.HB_JobId) && cachedSchedInfoHbEvent.isPresent()) {
                return cachedSchedInfoHbEvent;
            }
            return Optional.ofNullable(ServerSentEvent.create(mapper.writeValueAsString(jsi)));
        } catch (JsonProcessingException e) {
            logger.warn("failed to serialize Job Scheduling Info {}", jsi);
        }
        return Optional.empty();
    }

    public static Optional<ServerSentEvent> from(final JobClusterInfo jci) {
        try {
            if (jci.getName().equals(JobSchedulingInfo.HB_JobId) &&
                cachedJobClusterInfoHbEvent.isPresent()) {
                return cachedJobClusterInfoHbEvent;
            }
            return Optional.ofNullable(ServerSentEvent.create(mapper.writeValueAsString(jci)));
        } catch (JsonProcessingException e) {
            logger.warn("failed to serialize Job Cluster Info {}", jci);
        }
        return Optional.empty();
    }
}
