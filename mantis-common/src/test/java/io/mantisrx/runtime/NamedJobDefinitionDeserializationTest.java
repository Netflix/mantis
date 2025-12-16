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

package io.mantisrx.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.Test;

public class NamedJobDefinitionDeserializationTest {

    private static final ObjectMapper objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new Jdk8Module());

    @Test
    public void testDeserializeNamedJobDefinitionWithJobPrincipal() throws Exception {
        String json = "{\n" +
            "    \"jobDefinition\": {\n" +
            "        \"name\": \"TestGigi2\",\n" +
            "        \"user\": \"ggao\",\n" +
            "        \"jobJarFileLocation\": \"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/titus-migration-thin-1.2.3-snapshot.202511202319+aa2e763.zip\",\n" +
            "        \"version\": \"1.2.3-snapshot.202511202319+aa2e763\",\n" +
            "        \"schedulingInfo\": {\n" +
            "            \"stages\": {\n" +
            "                \"1\": {\n" +
            "                    \"numberOfInstances\": \"1\",\n" +
            "                    \"machineDefinition\": {\n" +
            "                        \"cpuCores\": 2,\n" +
            "                        \"memoryMB\": 8192,\n" +
            "                        \"diskMB\": 8192,\n" +
            "                        \"networkMbps\": 700,\n" +
            "                        \"numPorts\": \"1\"\n" +
            "                    },\n" +
            "                    \"scalable\": false,\n" +
            "                    \"softConstraints\": [],\n" +
            "                    \"hardConstraints\": [],\n" +
            "                    \"containerAttributes\": {\n" +
            "                        \"containerSkuID\": \"small\"\n" +
            "                    }\n" +
            "                }\n" +
            "            }\n" +
            "        },\n" +
            "        \"parameters\": [],\n" +
            "        \"labels\": [\n" +
            "            {\n" +
            "                \"name\": \"_mantis.user\",\n" +
            "                \"value\": \"ggao\"\n" +
            "            },\n" +
            "            {\n" +
            "                \"name\": \"_mantis.ownerEmail\",\n" +
            "                \"value\": \"ggao@netflix.com\"\n" +
            "            },\n" +
            "            {\n" +
            "                \"name\": \"_mantis.artifact\",\n" +
            "                \"value\": \"titus-migration-thin-1.2.3\"\n" +
            "            },\n" +
            "            {\n" +
            "                \"name\": \"_mantis.artifact.version\",\n" +
            "                \"value\": \"snapshot\"\n" +
            "            }\n" +
            "        ],\n" +
            "        \"migrationConfig\": {\n" +
            "            \"strategy\": \"PERCENTAGE\",\n" +
            "            \"configString\": \"{\\\"percentToMove\\\":25,\\\"intervalMs\\\":60000}\"\n" +
            "        },\n" +
            "        \"slaMin\": \"0\",\n" +
            "        \"slaMax\": \"0\",\n" +
            "        \"deploymentStrategy\": {\n" +
            "            \"resourceClusterId\": \"mantisResourceClusterStagingDefault3\"\n" +
            "        },\n" +
            "        \"cronSpec\": null,\n" +
            "        \"cronPolicy\": \"KEEP_EXISTING\"\n" +
            "    },\n" +
            "    \"owner\": {\n" +
            "        \"contactEmail\": \"ggao@netflix.com\",\n" +
            "        \"description\": \"\",\n" +
            "        \"name\": \"Gigi Gao\",\n" +
            "        \"repo\": \"\",\n" +
            "        \"teamName\": \"\"\n" +
            "    },\n" +
            "    \"jobPrincipal\": {\n" +
            "        \"email\": \"ggao@netflix.com\"\n" +
            "    }\n" +
            "}";

        NamedJobDefinition njd = objectMapper.readValue(json, NamedJobDefinition.class);

        // Verify the object was deserialized
        assertNotNull("NamedJobDefinition should not be null", njd);
        assertNotNull("JobDefinition should not be null", njd.getJobDefinition());
        assertNotNull("Owner should not be null", njd.getOwner());

        // Verify jobPrincipal was deserialized correctly
        assertNotNull("JobPrincipal should not be null", njd.getJobPrincipal());
        assertEquals("JobPrincipal email should match", "ggao@netflix.com", njd.getJobPrincipal().getEmail());
        assertEquals("JobPrincipal type should default to USER", JobPrincipal.PrincipalType.USER, njd.getJobPrincipal().getType());

        // Verify job definition fields
        assertEquals("Job name should match", "TestGigi2", njd.getJobDefinition().getName());
        assertEquals("User should match", "ggao", njd.getJobDefinition().getUser());

        // Verify owner fields
        assertEquals("Owner email should match", "ggao@netflix.com", njd.getOwner().getContactEmail());
        assertEquals("Owner name should match", "Gigi Gao", njd.getOwner().getName());
    }

    @Test
    public void testDeserializeNamedJobDefinitionWithoutJobPrincipal() throws Exception {
        // Test that jobPrincipal can be null for backward compatibility
        String json = "{\n" +
            "    \"jobDefinition\": {\n" +
            "        \"name\": \"TestJob\",\n" +
            "        \"user\": \"testuser\",\n" +
            "        \"jobJarFileLocation\": \"https://example.com/test.zip\",\n" +
            "        \"version\": \"1.0.0\",\n" +
            "        \"schedulingInfo\": {\n" +
            "            \"stages\": {\n" +
            "                \"1\": {\n" +
            "                    \"numberOfInstances\": \"1\",\n" +
            "                    \"machineDefinition\": {\n" +
            "                        \"cpuCores\": 1,\n" +
            "                        \"memoryMB\": 1024,\n" +
            "                        \"diskMB\": 1024,\n" +
            "                        \"networkMbps\": 128,\n" +
            "                        \"numPorts\": \"1\"\n" +
            "                    },\n" +
            "                    \"scalable\": false\n" +
            "                }\n" +
            "            }\n" +
            "        },\n" +
            "        \"parameters\": [],\n" +
            "        \"labels\": []\n" +
            "    },\n" +
            "    \"owner\": {\n" +
            "        \"contactEmail\": \"test@example.com\",\n" +
            "        \"description\": \"\",\n" +
            "        \"name\": \"Test User\",\n" +
            "        \"repo\": \"\",\n" +
            "        \"teamName\": \"\"\n" +
            "    }\n" +
            "}";

        NamedJobDefinition njd = objectMapper.readValue(json, NamedJobDefinition.class);

        // Verify the object was deserialized
        assertNotNull("NamedJobDefinition should not be null", njd);
        assertNotNull("JobDefinition should not be null", njd.getJobDefinition());
        assertNotNull("Owner should not be null", njd.getOwner());

        // JobPrincipal can be null for backward compatibility
        // This ensures old payloads without jobPrincipal still work
    }

}
