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

import io.mantisrx.server.core.Status;
import io.mantisrx.server.core.StatusPayloads;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;


public class HeartbeatTest {

    @Test
    public void testSingleUsePayloads() throws Exception {
        Heartbeat heartbeat = new Heartbeat("Jobcluster-123", 1, 0, 0);
        heartbeat.setPayload("" + StatusPayloads.Type.SubscriptionState, "true");
        int val1 = 10;
        int val2 = 12;
        heartbeat.addSingleUsePayload("" + StatusPayloads.Type.IncomingDataDrop, "" + val1);
        heartbeat.addSingleUsePayload("" + StatusPayloads.Type.IncomingDataDrop, "" + val2);
        final Status currentHeartbeatStatus = heartbeat.getCurrentHeartbeatStatus();
        List<Status.Payload> payloads = currentHeartbeatStatus.getPayloads();
        Assert.assertEquals(2, payloads.size());
        int value = 0;
        for (Status.Payload p : payloads) {
            if (StatusPayloads.Type.valueOf(p.getType()) == StatusPayloads.Type.IncomingDataDrop)
                value = Integer.parseInt(p.getData());
        }
        Assert.assertEquals(val2, value);
        payloads = heartbeat.getCurrentHeartbeatStatus().getPayloads();
        Assert.assertEquals(1, payloads.size());
    }
}
