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

package io.mantisrx.server.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.common.WorkerPorts;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.descriptor.SchedulingInfo;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.net.URL;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

public class ExecuteStageRequestTest {
    private ExecuteStageRequest example1;
    private ExecuteStageRequest example2;

    private final JsonSerializer serializer = new JsonSerializer();

    @Before
    public void setup() throws Exception {
        example1 = new ExecuteStageRequest("jobName", "jobId-0", 0, 1,
            new URL("http://datamesh/whatever"),
            1, 1,
            ImmutableList.of(1, 2, 3, 4, 5), 100L, 1,
            ImmutableList.of(new Parameter("name", "value")),
            new SchedulingInfo.Builder().numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1, 10, 10, 10, 2),
                    Lists.newArrayList(), Lists.newArrayList()).build(),
            MantisJobDurationType.Perpetual,
            0,
            1000L,
            1L,
            new WorkerPorts(2, 3, 4, 5, 6),
            java.util.Optional.of("className"),
            "user1",
            "111");
        example2 = new ExecuteStageRequest("jobName", "jobId-0", 0, 1,
            new URL("http://datamesh/whatever"),
            1, 1,
            ImmutableList.of(1, 2, 3, 4, 5), 100L, 1,
            ImmutableList.of(new Parameter("name", "value")),
            new SchedulingInfo.Builder().numberOfStages(1)
                .singleWorkerStageWithConstraints(new MachineDefinition(1, 10, 10, 10, 2),
                    Lists.newArrayList(), Lists.newArrayList()).build(),
            MantisJobDurationType.Perpetual,
            0,
            1000L,
            1L,
            new WorkerPorts(2, 3, 4, 5, 6),
            java.util.Optional.empty(),
            "user1",
            "111");
    }

    @Test
    public void testSerialization() throws Exception {
        byte[] serializedBytes = SerializationUtils.serialize(example1);
        assertTrue(serializedBytes.length > 0);
    }

    @Test
    public void testIfExecuteStageRequestIsSerializableAndDeserializableFromJackson() throws Exception {
        String json = serializer.toJson(example1);
        assertEquals(StringUtils.deleteWhitespace("{\n" +
            "    \"jobName\": \"jobName\",\n" +
            "    \"workerIndex\": 0,\n" +
            "    \"workerNumber\": 1,\n" +
            "    \"jobJarUrl\": \"http://datamesh/whatever\",\n" +
            "    \"stage\": 1,\n" +
            "    \"totalNumStages\": 1,\n" +
            "    \"ports\":\n" +
            "    [\n" +
            "        1,\n" +
            "        2,\n" +
            "        3,\n" +
            "        4,\n" +
            "        5\n" +
            "    ],\n" +
            "    \"timeoutToReportStart\": 100,\n" +
            "    \"metricsPort\": 1,\n" +
            "    \"parameters\":\n" +
            "    [\n" +
            "        {\n" +
            "            \"name\": \"name\",\n" +
            "            \"value\": \"value\"\n" +
            "        }\n" +
            "    ],\n" +
            "    \"schedulingInfo\":\n" +
            "    {\n" +
            "        \"stages\":\n" +
            "        {\n" +
            "            \"1\":\n" +
            "            {\n" +
            "                \"numberOfInstances\": 1,\n" +
            "                \"machineDefinition\":\n" +
            "                {\n" +
            "                    \"cpuCores\": 1.0,\n" +
            "                    \"memoryMB\": 10.0,\n" +
            "                    \"networkMbps\": 10.0,\n" +
            "                    \"diskMB\": 10.0,\n" +
            "                    \"numPorts\": 2\n" +
            "                },\n" +
            "                \"hardConstraints\":\n" +
            "                [],\n" +
            "                \"softConstraints\":\n" +
            "                [],\n" +
            "                \"scalingPolicy\": null,\n" +
            "                \"scalable\": false\n" +
            "            }\n" +
            "        }\n" +
            "    },\n" +
            "    \"durationType\": \"Perpetual\",\n" +
            "    \"heartbeatIntervalSecs\": 20,\n" +
            "    \"subscriptionTimeoutSecs\": 1000,\n" +
            "    \"minRuntimeSecs\": 1,\n" +
            "    \"workerPorts\":\n" +
            "    {\n" +
            "        \"metricsPort\": 2,\n" +
            "        \"debugPort\": 3,\n" +
            "        \"consolePort\": 4,\n" +
            "        \"customPort\": 5,\n" +
            "        \"ports\":\n" +
            "        [\n" +
            "            6\n" +
            "        ],\n" +
            "        \"sinkPort\": 6\n" +
            "    },\n" +
            "    \"nameOfJobProviderClass\": \"className\",\n" +
            "    \"user\": \"user1\",\n" +
            "    \"jobVersion\": \"111\",\n" +
            "    \"hasJobMaster\": false,\n" +
            "    \"jobId\": \"jobId-0\",\n" +
            "    \"workerId\":\n" +
            "    {\n" +
            "        \"jobCluster\": \"jobId\",\n" +
            "        \"jobId\": \"jobId-0\",\n" +
            "        \"workerIndex\": 0,\n" +
            "        \"workerNum\": 1\n" +
            "    }\n" +
            "}"), json);
        ExecuteStageRequest deserialized = serializer.fromJSON(json, ExecuteStageRequest.class);
        assertEquals(example1, deserialized);
    }

    @Test
    public void testIfExecuteStageRequestIsSerializableAndDeserializableFromJacksonWhenJobProviderClassIsEmpty() throws Exception {
        String json = serializer.toJson(example2);
        assertEquals(StringUtils.deleteWhitespace("{\n" +
            "    \"jobName\": \"jobName\",\n" +
            "    \"workerIndex\": 0,\n" +
            "    \"workerNumber\": 1,\n" +
            "    \"jobJarUrl\": \"http://datamesh/whatever\",\n" +
            "    \"stage\": 1,\n" +
            "    \"totalNumStages\": 1,\n" +
            "    \"ports\":\n" +
            "    [\n" +
            "        1,\n" +
            "        2,\n" +
            "        3,\n" +
            "        4,\n" +
            "        5\n" +
            "    ],\n" +
            "    \"timeoutToReportStart\": 100,\n" +
            "    \"metricsPort\": 1,\n" +
            "    \"parameters\":\n" +
            "    [\n" +
            "        {\n" +
            "            \"name\": \"name\",\n" +
            "            \"value\": \"value\"\n" +
            "        }\n" +
            "    ],\n" +
            "    \"schedulingInfo\":\n" +
            "    {\n" +
            "        \"stages\":\n" +
            "        {\n" +
            "            \"1\":\n" +
            "            {\n" +
            "                \"numberOfInstances\": 1,\n" +
            "                \"machineDefinition\":\n" +
            "                {\n" +
            "                    \"cpuCores\": 1.0,\n" +
            "                    \"memoryMB\": 10.0,\n" +
            "                    \"networkMbps\": 10.0,\n" +
            "                    \"diskMB\": 10.0,\n" +
            "                    \"numPorts\": 2\n" +
            "                },\n" +
            "                \"hardConstraints\":\n" +
            "                [],\n" +
            "                \"softConstraints\":\n" +
            "                [],\n" +
            "                \"scalingPolicy\": null,\n" +
            "                \"scalable\": false\n" +
            "            }\n" +
            "        }\n" +
            "    },\n" +
            "    \"durationType\": \"Perpetual\",\n" +
            "    \"heartbeatIntervalSecs\": 20,\n" +
            "    \"subscriptionTimeoutSecs\": 1000,\n" +
            "    \"minRuntimeSecs\": 1,\n" +
            "    \"workerPorts\":\n" +
            "    {\n" +
            "        \"metricsPort\": 2,\n" +
            "        \"debugPort\": 3,\n" +
            "        \"consolePort\": 4,\n" +
            "        \"customPort\": 5,\n" +
            "        \"ports\":\n" +
            "        [\n" +
            "            6\n" +
            "        ],\n" +
            "        \"sinkPort\": 6\n" +
            "    },\n" +
            "    \"nameOfJobProviderClass\": null,\n" +
            "    \"user\": \"user1\",\n" +
            "    \"jobVersion\": \"111\",\n" +
            "    \"hasJobMaster\": false,\n" +
            "    \"jobId\": \"jobId-0\",\n" +
            "    \"workerId\":\n" +
            "    {\n" +
            "        \"jobCluster\": \"jobId\",\n" +
            "        \"jobId\": \"jobId-0\",\n" +
            "        \"workerIndex\": 0,\n" +
            "        \"workerNum\": 1\n" +
            "    }\n" +
            "}"), json);
        ExecuteStageRequest deserialized = serializer.fromJSON(json, ExecuteStageRequest.class);
        assertEquals(example2, deserialized);
    }
}
