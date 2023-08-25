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

package io.mantisrx.server.worker;

import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.server.core.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nullable;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class Heartbeat {

    private static final Logger logger = LoggerFactory.getLogger(Heartbeat.class);
    @Getter
    private final String jobId;
    @Getter
    private final int stageNumber;
    @Getter
    private final int workerIndex;
    @Getter
    private final int workerNumber;
    private final ConcurrentMap<String, String> payloads;
    private final BlockingQueue<PayloadPair> singleUsePayloads = new LinkedBlockingQueue<>();
    private final Optional<String> host;

    Heartbeat(String jobId, int stageNumber, int workerIndex, int workerNumber) {
        this(jobId, stageNumber, workerIndex, workerNumber, null);
    }

    Heartbeat(String jobId, int stageNumber, int workerIndex, int workerNumber, @Nullable String host) {
        this.jobId = jobId;
        this.stageNumber = stageNumber;
        this.workerIndex = workerIndex;
        this.workerNumber = workerNumber;
        this.host = Optional.ofNullable(host);
        payloads = new ConcurrentHashMap<>();
    }

    void setPayload(String name, String value) {
        logger.info("Setting payload " + name);
        if (name != null && !name.isEmpty() && value != null)
            payloads.put(name, value);
    }

    boolean clearPayload(String name) {
        return payloads.remove(name) != null;
    }

    void addSingleUsePayload(String name, String value) {
        logger.debug("Adding payload {}={}", name, value);
        singleUsePayloads.offer(new PayloadPair(name, value));
    }

    Status getCurrentHeartbeatStatus() {
        List<Status.Payload> payloadList = new ArrayList<>();
        logger.debug("#Payloads = " + payloads.size());
        for (Map.Entry<String, String> entry : payloads.entrySet()) {
            logger.debug("Adding payload " + entry.getKey() + " with value " + entry.getValue());
            payloadList.add(new Status.Payload(entry.getKey(), entry.getValue()));
        }
        List<PayloadPair> singleUsePlds = new ArrayList<>();
        singleUsePayloads.drainTo(singleUsePlds);
        if (!singleUsePlds.isEmpty()) {
            Map<String, String> suplds = new HashMap<>();
            for (PayloadPair pp : singleUsePlds)
                suplds.put(pp.name, pp.value); // eliminates duplicates, keeps the last one
            for (Map.Entry<String, String> entry : suplds.entrySet())
                payloadList.add(new Status.Payload(entry.getKey(), entry.getValue()));
        }
        Status status = new Status(jobId, stageNumber, workerIndex, workerNumber, Status.TYPE.HEARTBEAT, "heartbeat", MantisJobState.Noop);
        host.ifPresent(status::setHostname);
        if (!payloadList.isEmpty())
            status.setPayloads(payloadList);
        return status;
    }

    private static class PayloadPair {

        String name;
        String value;

        public PayloadPair(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
}
