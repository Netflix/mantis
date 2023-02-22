/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.discovery.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;


public class AppJobClustersMapTest {

    @Test
    public void testSerDe() throws IOException {
        Map<String, Object> mappings = new HashMap<>();
        Map<String, Object> defaultAppMappings = new HashMap<>();
        defaultAppMappings.put(StreamJobClusterMap.DEFAULT_STREAM_KEY, "SharedPushEventSource");
        defaultAppMappings.put("testEventStream", "TestPushEventSource");
        mappings.put(AppJobClustersMap.DEFAULT_APP_KEY, defaultAppMappings);

        Map<String, Object> customAppMappings = new HashMap<>();
        customAppMappings.put("customEventStream", "CustomPushEventSource");
        customAppMappings.put(StreamJobClusterMap.DEFAULT_STREAM_KEY, "CustomDefaultPushEventSource");
        mappings.put("custom", customAppMappings);

        AppJobClustersMap mapV1 = new AppJobClustersMap(AppJobClustersMap.VERSION_1, System.currentTimeMillis(), mappings);

        assertEquals("SharedPushEventSource", mapV1.getStreamJobClusterMap(AppJobClustersMap.DEFAULT_APP_KEY).getJobCluster(StreamJobClusterMap.DEFAULT_STREAM_KEY));
        assertEquals("SharedPushEventSource", mapV1.getStreamJobClusterMap(AppJobClustersMap.DEFAULT_APP_KEY).getJobCluster("AnyRandomStream"));
        assertEquals("TestPushEventSource", mapV1.getStreamJobClusterMap(AppJobClustersMap.DEFAULT_APP_KEY).getJobCluster("testEventStream"));

        StreamJobClusterMap customStreamJCMap = mapV1.getStreamJobClusterMap("custom");
        System.out.println("custom app mapping: " + customStreamJCMap);

        assertTrue(customStreamJCMap.getStreamJobClusterMap().size() == 2);
        assertTrue(customStreamJCMap.getStreamJobClusterMap().containsKey(StreamJobClusterMap.DEFAULT_STREAM_KEY));
        assertFalse(customStreamJCMap.getStreamJobClusterMap().containsKey("testEventStream"));
        assertTrue(customStreamJCMap.getStreamJobClusterMap().containsKey("customEventStream"));

        assertEquals("CustomDefaultPushEventSource", customStreamJCMap.getJobCluster(StreamJobClusterMap.DEFAULT_STREAM_KEY));
        assertEquals("CustomDefaultPushEventSource", customStreamJCMap.getJobCluster("AnyRandomStreamName"));
        assertEquals("CustomDefaultPushEventSource", customStreamJCMap.getJobCluster("testEventStream"));
        assertEquals("CustomPushEventSource", customStreamJCMap.getJobCluster("customEventStream"));

        ObjectMapper mapper = new ObjectMapper();
        String mappingStr = mapper.writeValueAsString(mapV1);

        System.out.println("input mappings " + mappingStr);
        AppJobClustersMap appJobClustersMap = mapper.readValue(mappingStr, AppJobClustersMap.class);

        System.out.println("parsed mappings " + appJobClustersMap);

        assertEquals(mapV1, appJobClustersMap);

    }

    @Test
    public void testDeSerFromString() throws IOException {
        final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        final String mapping = "{\"version\":\"1\",\"timestamp\":2,\"mappings\":{\"testApp\":{\"__default__\":\"SharedMrePublishEventSource\"}}}";
        AppJobClustersMap appJobClustersMap = mapper.readValue(mapping, AppJobClustersMap.class);
        assertEquals(2, appJobClustersMap.getTimestamp());
        assertEquals("testApp", appJobClustersMap.getStreamJobClusterMap("testApp").getAppName());
        assertEquals("SharedMrePublishEventSource", appJobClustersMap.getStreamJobClusterMap("testApp").getJobCluster("testStream"));
    }
}
