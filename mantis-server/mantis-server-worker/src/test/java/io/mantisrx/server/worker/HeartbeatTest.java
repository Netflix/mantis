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

import static org.junit.Assert.assertEquals;

import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.StatusPayloads;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;


public class HeartbeatTest {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(HeartbeatTest.class);

    @Test
    public void testSingleUsePayloads() {
        Heartbeat heartbeat = new Heartbeat("Jobcluster-123", 1, 0, 0);
        heartbeat.setPayload("" + StatusPayloads.Type.SubscriptionState, "true");
        int val1 = 10;
        int val2 = 12;
        heartbeat.addSingleUsePayload("" + StatusPayloads.Type.IncomingDataDrop, "" + val1);
        heartbeat.addSingleUsePayload("" + StatusPayloads.Type.IncomingDataDrop, "" + val2);
        List<Status.Payload> payloads = heartbeat.getCurrentHeartbeatStatus().getPayloads();

        logger.debug("Current Payloads: {}", payloads);
        assertEquals(2, payloads.size());

        int value = 0;
        for (Status.Payload p : payloads) {
            if (StatusPayloads.Type.valueOf(p.getType()) == StatusPayloads.Type.IncomingDataDrop)
                value = Integer.parseInt(p.getData());
        }
        assertEquals(val2, value);

        payloads = heartbeat.getCurrentHeartbeatStatus().getPayloads();
        logger.debug("Payloads after draining single-use payloads: {}", payloads);
        assertEquals(1, payloads.size());
    }
}
