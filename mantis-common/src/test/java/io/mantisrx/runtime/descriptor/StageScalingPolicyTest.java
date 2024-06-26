/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.runtime.descriptor;

import static org.junit.Assert.assertEquals;

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.runtime.descriptor.StageScalingPolicy.RollingCount;
import io.mantisrx.runtime.descriptor.StageScalingPolicy.ScalingReason;
import io.mantisrx.runtime.descriptor.StageScalingPolicy.Strategy;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class StageScalingPolicyTest {
    private final JsonSerializer serializer = new JsonSerializer();

    @Test
    public void testSerialization() throws Exception {
        Map<ScalingReason, Strategy> smap = new HashMap<>();
        smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 0.5, 0.75, null));
        smap.put(ScalingReason.DataDrop, new Strategy(ScalingReason.DataDrop, 0.0, 2.0, null));
        StageScalingPolicy policy = new StageScalingPolicy(1, 1, 2, 1, 1, 60, smap, false);

        final String expected = "{\n" +
            "    \"stage\": 1,\n" +
            "    \"min\": 1,\n" +
            "    \"max\": 2,\n" +
            "    \"increment\": 1,\n" +
            "    \"decrement\": 1,\n" +
            "    \"coolDownSecs\": 60,\n" +
            "    \"strategies\":\n" +
            "    {\n" +
            "        \"CPU\":\n" +
            "        {\n" +
            "            \"reason\": \"CPU\",\n" +
            "            \"scaleDownBelowPct\": 0.5,\n" +
            "            \"scaleUpAbovePct\": 0.75,\n" +
            "            \"rollingCount\":\n" +
            "            {\n" +
            "                \"count\": 1,\n" +
            "                \"of\": 1\n" +
            "            }\n" +
            "        },\n" +
            "        \"DataDrop\":\n" +
            "        {\n" +
            "            \"reason\": \"DataDrop\",\n" +
            "            \"scaleDownBelowPct\": 0.0,\n" +
            "            \"scaleUpAbovePct\": 2.0,\n" +
            "            \"rollingCount\":\n" +
            "            {\n" +
            "                \"count\": 1,\n" +
            "                \"of\": 1\n" +
            "            }\n" +
            "        }\n" +
            "    },\n" +
            "    \"enabled\": true\n" +
            "}";
        StageScalingPolicy actual =
            serializer.fromJson(expected.getBytes(StandardCharsets.UTF_8), StageScalingPolicy.class);
        assertEquals(policy, actual);
    }

    @Test
    public void testDeserialization() throws Exception {
        String json1 = "{\"stage\":1,\"min\":1,\"max\":2,\"increment\":1,\"decrement\":1,\"strategies\":{},\"enabled\":false}";
        StageScalingPolicy actual = serializer.fromJSON(json1, StageScalingPolicy.class);
        StageScalingPolicy expected = new StageScalingPolicy(1, 1, 2, 1, 1, 0, null, false);
        assertEquals(expected, actual);

        String json2 = "{\"stage\":1,\"min\":1,\"max\":5,\"increment\":1,\"decrement\":1,\"coolDownSecs\":600,\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":50,\"scaleUpAbovePct\":75}},\"enabled\":true}";
        actual = serializer.fromJSON(json2, StageScalingPolicy.class);
        Map<ScalingReason, Strategy> smap = new HashMap<>();
        smap.put(ScalingReason.CPU, new Strategy(ScalingReason.CPU, 50, 75.0, new RollingCount(1, 1)));
        expected = new StageScalingPolicy(1, 1, 5, 1, 1, 600, smap, false);
        assertEquals(expected, actual);

        String json3 = "{\"stage\":1,\"min\":1,\"max\":3,\"increment\":1,\"decrement\":1,\"coolDownSecs\":0,\"strategies\":{\"Memory\":{\"reason\":\"Memory\",\"scaleDownBelowPct\":65,\"scaleUpAbovePct\":80,\"rollingCount\":{\"count\":6,\"of\":10}}},\"enabled\":true}";
        actual = serializer.fromJSON(json3, StageScalingPolicy.class);
        smap = new HashMap<>();
        smap.put(ScalingReason.Memory, new Strategy(ScalingReason.Memory, 65, 80.0, new RollingCount(6, 10)));
        expected = new StageScalingPolicy(1, 1, 3, 1, 1, 0, smap, false);
        assertEquals(expected, actual);

        String json4 = "{\"stage\":1,\"min\":1,\"max\":3,\"increment\":1,\"decrement\":1,\"coolDownSecs\":0,\"strategies\":{\"Memory\":{\"reason\":\"Memory\",\"scaleDownBelowPct\":65,\"scaleUpAbovePct\":80,\"rollingCount\":{\"count\":6,\"of\":10}}},\"allowAutoScaleManager\":false,\"enabled\":true}";
        actual = serializer.fromJSON(json4, StageScalingPolicy.class);
        smap = new HashMap<>();
        smap.put(ScalingReason.Memory, new Strategy(ScalingReason.Memory, 65, 80.0, new RollingCount(6, 10)));
        expected = new StageScalingPolicy(1, 1, 3, 1, 1, 0, smap, false);
        assertEquals(expected, actual);

        String json5 = "{\"stage\":1,\"min\":1,\"max\":3,\"increment\":1,\"decrement\":1,\"coolDownSecs\":0,\"strategies\":{\"Memory\":{\"reason\":\"Memory\",\"scaleDownBelowPct\":65,\"scaleUpAbovePct\":80,\"rollingCount\":{\"count\":6,\"of\":10}}},\"allowAutoScaleManager\":true,\"enabled\":true}";
        actual = serializer.fromJSON(json5, StageScalingPolicy.class);
        smap = new HashMap<>();
        smap.put(ScalingReason.Memory, new Strategy(ScalingReason.Memory, 65, 80.0, new RollingCount(6, 10)));
        expected = new StageScalingPolicy(1, 1, 3, 1, 1, 0, smap, true);
        assertEquals(expected, actual);
    }
}
