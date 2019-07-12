/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.publish.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mantisrx.common.utils.MantisSourceJobConstants;
import org.junit.jupiter.api.Test;
import rx.functions.Func1;


public class EventFilterTest {

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void missingClientIdFails() {
        try {
            new EventFilter(null);
            fail();
        } catch (Exception ignored) {
        }
    }

    @Test
    public void basicFilterTest() throws JsonProcessingException {
        String clientId = "myClientId";
        EventFilter filter = new EventFilter(clientId);

        Map<String, List<String>> params = new HashMap<>();
        List<String> subIdParam = new ArrayList<>();

        subIdParam.add("mySubId");

        params.put(MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME, subIdParam);

        Func1<String, Boolean> materializedFilter = filter.call(params);

        List<String> matchedClients = new ArrayList<>();
        matchedClients.add(clientId + "_" + "mySubId");
        matchedClients.add(clientId + "_" + "BlahSubId");

        Map<String, Object> payLoad = new HashMap<>();
        payLoad.put("ts", System.currentTimeMillis());
        payLoad.put("matchedClients", matchedClients);
        payLoad.put("type", "EVENT");

        String payloadStr = mapper.writeValueAsString(payLoad);

        assertTrue(materializedFilter.call(payloadStr));

        List<String> matchedClients2 = new ArrayList<>();
        matchedClients.add(clientId + "_" + "mySubId2");
        matchedClients.add(clientId + "_" + "BlahSubId");

        payLoad = new HashMap<>();
        payLoad.put("ts", System.currentTimeMillis());
        payLoad.put("matchedClients", matchedClients2);
        payLoad.put("type", "EVENT");

        payloadStr = mapper.writeValueAsString(payLoad);

        assertFalse(materializedFilter.call(payloadStr));
    }

    @Test
    public void basicEmptyEventFilterTest() throws JsonProcessingException {
        String clientId = "myClientId";
        EventFilter filter = new EventFilter(clientId);

        Map<String, List<String>> params = new HashMap<>();
        List<String> subIdParam = new ArrayList<>();

        subIdParam.add("mySubId");

        params.put(MantisSourceJobConstants.SUBSCRIPTION_ID_PARAM_NAME, subIdParam);

        Func1<String, Boolean> materializedFilter = filter.call(params);

        List<String> matchedClients = new ArrayList<>();
        matchedClients.add(clientId + "_" + "mySubId");
        matchedClients.add(clientId + "_" + "BlahSubId");

        Map<String, Object> payLoad = new HashMap<>();

        String payloadStr = mapper.writeValueAsString(payLoad);

        assertFalse(materializedFilter.call(payloadStr));

        try {
            assertFalse(materializedFilter.call(null));
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void missingSubIdParamAlwaysPasses() throws JsonProcessingException {
        String clientId = "myClientId";
        EventFilter filter = new EventFilter(clientId);

        Map<String, List<String>> params = new HashMap<>();

        Func1<String, Boolean> materializedFilter = filter.call(params);

        List<String> matchedClients = new ArrayList<>();
        matchedClients.add(clientId + "_" + "mySubId");
        matchedClients.add(clientId + "_" + "BlahSubId");

        Map<String, Object> payLoad = new HashMap<>();
        payLoad.put("ts", System.currentTimeMillis());
        payLoad.put("matchedClients", matchedClients);
        payLoad.put("type", "EVENT");

        String payloadStr = mapper.writeValueAsString(payLoad);

        assertTrue(materializedFilter.call(payloadStr));
    }
}
