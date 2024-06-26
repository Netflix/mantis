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

package io.mantisrx.runtime.descriptor;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.descriptor.SchedulingInfo.Builder;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class SchedulingInfoTest {
    @Test
    public void buildWithInheritInstanceTest() {
        Map<Integer, StageSchedulingInfo> givenStages = new HashMap<>();
        givenStages.put(1, StageSchedulingInfo.builder().numberOfInstances(1).build());
        SchedulingInfo.Builder builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> empty(),
                        i -> false,
                        false);
        SchedulingInfo info = builder.build();
        assertEquals(1, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());

        givenStages.put(2, StageSchedulingInfo.builder().numberOfInstances(2).build());
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> empty(),
                        i -> false,
                        false);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test valid existing count + no inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> false,
                        false);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test invalid existing count + inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> empty(),
                        i -> true,
                        false);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test invalid existing count + force inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> empty(),
                        i -> false,
                        true);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(1, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test valid existing count + inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> i == 1,
                        false);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(9, info.forStage(1).getNumberOfInstances());
        assertEquals(2, info.forStage(2).getNumberOfInstances());

        // test valid existing count + force inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> false,
                        true);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(9, info.forStage(1).getNumberOfInstances());
        assertEquals(9, info.forStage(2).getNumberOfInstances());

        // test valid existing count + both inherit flag
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> true,
                        true);
        info = builder.build();
        assertEquals(2, info.getStages().size());
        assertEquals(9, info.forStage(1).getNumberOfInstances());
        assertEquals(9, info.forStage(2).getNumberOfInstances());

        // test job master
        givenStages.put(0, StageSchedulingInfo.builder().numberOfInstances(1).build());
        builder = new SchedulingInfo.Builder()
                .createWithInstanceInheritance(
                        givenStages,
                        i -> of(9),
                        i -> true,
                        true);
        info = builder.build();
        assertEquals(3, info.getStages().size());
        assertEquals(9, info.forStage(0).getNumberOfInstances());
        assertEquals(9, info.forStage(1).getNumberOfInstances());
        assertEquals(9, info.forStage(2).getNumberOfInstances());

    }

    @Test
    public void testSerialization() throws Exception {
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
        smap.put(StageScalingPolicy.ScalingReason.Memory, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, 0.1, 0.6, null));
        Builder builder = new Builder()
            .numberOfStages(2)
            .multiWorkerScalableStageWithConstraints(
                2,
                new MachineDefinition(1, 1.24, 0.0, 1, 1),
                null, null,
                new StageScalingPolicy(1, 1, 3, 1, 1, 60, smap, false)
            )
            .multiWorkerScalableStageWithConstraints(
                3,
                new MachineDefinition(1, 1.24, 0.0, 1, 1),
                null, null,
                new StageScalingPolicy(1, 1, 3, 1, 1, 60, smap, true)
            );

        JsonSerializer serializer = new JsonSerializer();
        String expected = "" +
            "{" +
            "    \"stages\":" +
            "    {" +
            "        \"1\":" +
            "        {" +
            "            \"numberOfInstances\": 2," +
            "            \"machineDefinition\":" +
            "            {" +
            "                \"cpuCores\": 1.0," +
            "                \"memoryMB\": 1.24," +
            "                \"networkMbps\": 128.0," +
            "                \"diskMB\": 1.0," +
            "                \"numPorts\": 1" +
            "            }," +
            "            \"hardConstraints\":" +
            "            []," +
            "            \"softConstraints\":" +
            "            []," +
            "            \"scalingPolicy\":" +
            "            {" +
            "                \"stage\": 1," +
            "                \"min\": 1," +
            "                \"max\": 3," +
            "                \"increment\": 1," +
            "                \"decrement\": 1," +
            "                \"coolDownSecs\": 60," +
            "                \"strategies\":" +
            "                {" +
            "                    \"Memory\":" +
            "                    {" +
            "                        \"reason\": \"Memory\"," +
            "                        \"scaleDownBelowPct\": 0.1," +
            "                        \"scaleUpAbovePct\": 0.6," +
            "                        \"rollingCount\":" +
            "                        {" +
            "                            \"count\": 1," +
            "                            \"of\": 1" +
            "                        }" +
            "                    }" +
            "                }," +
            "                \"allowAutoScaleManager\": false," +
            "                \"enabled\": true" +
            "            }," +
            "            \"scalable\": true" +
            "        }," +
            "        \"2\":" +
            "        {" +
            "            \"numberOfInstances\": 3," +
            "            \"machineDefinition\":" +
            "            {" +
            "                \"cpuCores\": 1.0," +
            "                \"memoryMB\": 1.24," +
            "                \"networkMbps\": 128.0," +
            "                \"diskMB\": 1.0," +
            "                \"numPorts\": 1" +
            "            }," +
            "            \"hardConstraints\":" +
            "            []," +
            "            \"softConstraints\":" +
            "            []," +
            "            \"scalingPolicy\":" +
            "            {" +
            "                \"stage\": 2," +
            "                \"min\": 1," +
            "                \"max\": 3," +
            "                \"increment\": 1," +
            "                \"decrement\": 1," +
            "                \"coolDownSecs\": 60," +
            "                \"strategies\":" +
            "                {" +
            "                    \"Memory\":" +
            "                    {" +
            "                        \"reason\": \"Memory\"," +
            "                        \"scaleDownBelowPct\": 0.1," +
            "                        \"scaleUpAbovePct\": 0.6," +
            "                        \"rollingCount\":" +
            "                        {" +
            "                            \"count\": 1," +
            "                            \"of\": 1" +
            "                        }" +
            "                    }" +
            "                }," +
            "                \"allowAutoScaleManager\": true," +
            "                \"enabled\": true" +
            "            }," +
            "            \"scalable\": true" +
            "        }" +
            "    }" +
            "}";
        assertEquals(expected.replaceAll("\\s+", ""), serializer.toJson(builder.build()));
    }

    @Test
    public void testSerializationWithSkuId() throws Exception {
        Map<StageScalingPolicy.ScalingReason, StageScalingPolicy.Strategy> smap = new HashMap<>();
        smap.put(StageScalingPolicy.ScalingReason.Memory, new StageScalingPolicy.Strategy(StageScalingPolicy.ScalingReason.Memory, 0.1, 0.6, null));
        Builder builder = new Builder()
            .numberOfStages(2)
            .multiWorkerScalableStageWithConstraints(
                2,
                new MachineDefinition(1, 1.24, 0.0, 1, 1),
                null, null,
                new StageScalingPolicy(1, 1, 3, 1, 1, 60, smap, false),
                ImmutableMap.of("containerSkuID", "sku1")
            )
            .multiWorkerScalableStageWithConstraints(
                3,
                new MachineDefinition(1, 1.24, 0.0, 1, 1),
                null, null,
                new StageScalingPolicy(1, 1, 3, 1, 1, 60, smap, false),
                ImmutableMap.of("containerSkuID", "sku2")
            );

        JsonSerializer serializer = new JsonSerializer();
        String expected = "" +
            "{" +
            "    \"stages\":" +
            "    {" +
            "        \"1\":" +
            "        {" +
            "            \"numberOfInstances\": 2," +
            "            \"machineDefinition\":" +
            "            {" +
            "                \"cpuCores\": 1.0," +
            "                \"memoryMB\": 1.24," +
            "                \"networkMbps\": 128.0," +
            "                \"diskMB\": 1.0," +
            "                \"numPorts\": 1" +
            "            }," +
            "            \"hardConstraints\":" +
            "            []," +
            "            \"softConstraints\":" +
            "            []," +
            "            \"scalingPolicy\":" +
            "            {" +
            "                \"stage\": 1," +
            "                \"min\": 1," +
            "                \"max\": 3," +
            "                \"increment\": 1," +
            "                \"decrement\": 1," +
            "                \"coolDownSecs\": 60," +
            "                \"strategies\":" +
            "                {" +
            "                    \"Memory\":" +
            "                    {" +
            "                        \"reason\": \"Memory\"," +
            "                        \"scaleDownBelowPct\": 0.1," +
            "                        \"scaleUpAbovePct\": 0.6," +
            "                        \"rollingCount\":" +
            "                        {" +
            "                            \"count\": 1," +
            "                            \"of\": 1" +
            "                        }" +
            "                    }" +
            "                }," +
            "                \"allowAutoScaleManager\": false," +
            "                \"enabled\": true" +
            "            }," +
            "            \"scalable\": true," +
            "            \"containerAttributes\": {\"containerSkuID\":\"sku1\"}" +
            "        }," +
            "        \"2\":" +
            "        {" +
            "            \"numberOfInstances\": 3," +
            "            \"machineDefinition\":" +
            "            {" +
            "                \"cpuCores\": 1.0," +
            "                \"memoryMB\": 1.24," +
            "                \"networkMbps\": 128.0," +
            "                \"diskMB\": 1.0," +
            "                \"numPorts\": 1" +
            "            }," +
            "            \"hardConstraints\":" +
            "            []," +
            "            \"softConstraints\":" +
            "            []," +
            "            \"scalingPolicy\":" +
            "            {" +
            "                \"stage\": 2," +
            "                \"min\": 1," +
            "                \"max\": 3," +
            "                \"increment\": 1," +
            "                \"decrement\": 1," +
            "                \"coolDownSecs\": 60," +
            "                \"strategies\":" +
            "                {" +
            "                    \"Memory\":" +
            "                    {" +
            "                        \"reason\": \"Memory\"," +
            "                        \"scaleDownBelowPct\": 0.1," +
            "                        \"scaleUpAbovePct\": 0.6," +
            "                        \"rollingCount\":" +
            "                        {" +
            "                            \"count\": 1," +
            "                            \"of\": 1" +
            "                        }" +
            "                    }" +
            "                }," +
            "                \"allowAutoScaleManager\": false," +
            "                \"enabled\": true" +
            "            }," +
            "            \"scalable\": true," +
            "            \"containerAttributes\": {\"containerSkuID\":\"sku2\"}" +
            "        }" +
            "    }" +
            "}";
        assertEquals(expected.replaceAll("\\s+", ""), serializer.toJson(builder.build()));
    }
}
