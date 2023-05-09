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

package io.mantisrx.master.jobcluster.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.master.jobcluster.proto.JobClusterManagerProto.UpdateSchedulingInfoRequest;
import org.junit.Test;

public class JobClusterManagerProtoTest {
    @Test
    public void testDeserialization() throws Exception {
        String json = "{\n"
            + "    \"schedulingInfo\":\n"
            + "    {\n"
            + "        \"stages\":\n"
            + "        {\n"
            + "            \"1\":\n"
            + "            {\n"
            + "                \"numberOfInstances\": 1,\n"
            + "                \"machineDefinition\":\n"
            + "                {\n"
            + "                    \"cpuCores\": 1.0,\n"
            + "                    \"memoryMB\": 4094.0,\n"
            + "                    \"networkMbps\": 128.0,\n"
            + "                    \"diskMB\": 6000.0,\n"
            + "                    \"numPorts\": 1\n"
            + "                },\n"
            + "                \"hardConstraints\":\n"
            + "                [],\n"
            + "                \"softConstraints\":\n"
            + "                [],\n"
            + "                \"scalingPolicy\": null,\n"
            + "                \"scalable\": false\n"
            + "            },\n"
            + "            \"2\":\n"
            + "            {\n"
            + "                \"numberOfInstances\": 1,\n"
            + "                \"machineDefinition\":\n"
            + "                {\n"
            + "                    \"cpuCores\": 1.0,\n"
            + "                    \"memoryMB\": 4096.0,\n"
            + "                    \"networkMbps\": 128.0,\n"
            + "                    \"diskMB\": 6000.0,\n"
            + "                    \"numPorts\": 1\n"
            + "                },\n"
            + "                \"hardConstraints\":\n"
            + "                [],\n"
            + "                \"softConstraints\":\n"
            + "                [],\n"
            + "                \"scalingPolicy\": null,\n"
            + "                \"scalable\": false\n"
            + "            },\n"
            + "            \"3\":\n"
            + "            {\n"
            + "                \"numberOfInstances\": 1,\n"
            + "                \"machineDefinition\":\n"
            + "                {\n"
            + "                    \"cpuCores\": 1.0,\n"
            + "                    \"memoryMB\": 4096.0,\n"
            + "                    \"networkMbps\": 128.0,\n"
            + "                    \"diskMB\": 6000.0,\n"
            + "                    \"numPorts\": 1\n"
            + "                },\n"
            + "                \"hardConstraints\":\n"
            + "                [],\n"
            + "                \"softConstraints\":\n"
            + "                [],\n"
            + "                \"scalingPolicy\": null,\n"
            + "                \"scalable\": false\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"version\": \"0.0.1-snapshot.202303220002+fdichiara.runtimeV2.d16a200 2023-05-09 09:10:42\"\n"
            + "}";
        UpdateSchedulingInfoRequest result =
            Jackson.fromJSON(json, UpdateSchedulingInfoRequest.class);
        assertNotNull(result);
        assertEquals(result.getVersion(), "0.0.1-snapshot.202303220002+fdichiara.runtimeV2.d16a200 2023-05-09 09:10:42");

        result.getSchedulingInfo().getStages().values().forEach(stage -> {
            assertNotNull(stage.getMachineDefinition());
            assertNotNull(stage.getHardConstraints());
            assertNotNull(stage.getSoftConstraints());
        });
    }
}
