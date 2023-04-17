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

package io.mantisrx.master.api.akka.route;

import static org.junit.Assert.*;

import io.mantisrx.common.Ack;
import io.mantisrx.master.jobcluster.job.MantisJobMetadataView;
import io.mantisrx.server.master.store.MantisWorkerMetadataWritable;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Optional;
import org.junit.Test;


public class JacksonTest {
    private static final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    public void testDeser4() throws IOException {
        final String jsonStr = "[{\"jobMetadata\":{\"jobId\":\"sine-function-1\",\"name\":\"sine-function\"," +
            "\"user\":\"nmahilani\",\"submittedAt\":1527703650220,\"jarUrl\":\"https://mantis.staging.us-east-1.prod.netflix.net/mantis-artifacts/mantis-examples-sine-function-0.2.9.zip\"," +
            "\"numStages\":2,\"sla\":{\"runtimeLimitSecs\":0,\"minRuntimeSecs\":0,\"slaType\":\"Lossy\",\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"}," +
            "\"state\":\"Accepted\",\"subscriptionTimeoutSecs\":0,\"parameters\":[{\"name\":\"useRandom\",\"value\":\"True\"}],\"nextWorkerNumberToUse\":11," +
            "\"migrationConfig\":{\"strategy\":\"PERCENTAGE\",\"configString\":\"{\\\"percentToMove\\\":25,\\\"intervalMs\\\":60000}\"}," +
            "\"labels\":[{\"name\":\"_mantis.user\",\"value\":\"nmahilani\"},{\"name\":\"_mantis.ownerEmail\",\"value\":\"nmahilani@netflix.com\"}," +
            "{\"name\":\"_mantis.jobType\",\"value\":\"other\"},{\"name\":\"_mantis.criticality\",\"value\":\"low\"},{\"name\":\"_mantis.artifact.version\",\"value\":\"0.2.9\"}]}," +
            "\"stageMetadataList\":[{\"jobId\":\"sine-function-1\",\"stageNum\":0,\"numStages\":2,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":200.0,\"networkMbps\":128.0,\"diskMB\":1024.0,\"numPorts\":1}," +
            "\"numWorkers\":1,\"hardConstraints\":null,\"softConstraints\":null,\"scalingPolicy\":null,\"scalable\":false}," +
            "{\"jobId\":\"sine-function-1\",\"stageNum\":1,\"numStages\":2,\"machineDefinition\":{\"cpuCores\":1.0,\"memoryMB\":200.0,\"networkMbps\":128.0,\"diskMB\":1024.0,\"numPorts\":1},\"numWorkers\":1,\"hardConstraints\":[],\"softConstraints\":[\"M4Cluster\"]," +
            "\"scalingPolicy\":{\"stage\":1,\"min\":1,\"max\":10,\"increment\":2,\"decrement\":1,\"coolDownSecs\":600," +
            "\"strategies\":{\"CPU\":{\"reason\":\"CPU\",\"scaleDownBelowPct\":15.0,\"scaleUpAbovePct\":75.0,\"rollingCount\":{\"count\":12,\"of\":20}}},\"enabled\":true},\"scalable\":true}]," +
            "\"workerMetadataList\":[{\"workerIndex\":0,\"workerNumber\":2,\"jobId\":\"sine-function-1\",\"stageNum\":0,\"numberOfPorts\":4,\"metricsPort\":0,\"consolePort\":0," +
            "\"debugPort\":-1,\"ports\":[],\"state\":\"Accepted\",\"slave\":null,\"slaveID\":null,\"cluster\":{\"present\":false},\"acceptedAt\":1527703650231,\"launchedAt\":0,\"startingAt\":0,\"startedAt\":0," +
            "\"completedAt\":0,\"reason\":null,\"resubmitOf\":-1,\"totalResubmitCount\":0},{\"workerIndex\":0,\"workerNumber\":3,\"jobId\":\"sine-function-1\",\"stageNum\":1,\"numberOfPorts\":4,\"metricsPort\":0,\"consolePort\":0,\"debugPort\":-1,\"ports\":[],\"state\":\"Accepted\"," +
            "\"slave\":null,\"slaveID\":null,\"cluster\":{\"present\":false},\"acceptedAt\":1527703650232,\"launchedAt\":0,\"startingAt\":0,\"startedAt\":0,\"completedAt\":0," +
            "\"reason\":null,\"resubmitOf\":-1,\"totalResubmitCount\":0}]}]";

        final List<MantisJobMetadataView> jobIdInfos = Jackson.fromJSON(objectMapper, jsonStr, new TypeReference<List<MantisJobMetadataView>>() { });
        assertEquals(1, jobIdInfos.size());
        final MantisJobMetadataView jobInfo = jobIdInfos.get(0);
        assertEquals("sine-function-1", jobInfo.getJobMetadata().getJobId());
        assertEquals(2, jobInfo.getWorkerMetadataList().size());
        assertEquals(2, jobInfo.getStageMetadataList().size());

        MantisWorkerMetadataWritable mwm = jobInfo.getWorkerMetadataList().get(0);
        mwm.setCluster(Optional.of("test"));

        final String out = objectMapper.writer(Jackson.DEFAULT_FILTER_PROVIDER).writeValueAsString(mwm);
        assertTrue(out.contains("\"cluster\":{\"present\":true},"));
        final String serializeAgain = objectMapper.writeValueAsString(objectMapper.readValue(out, MantisWorkerMetadataWritable.class));
        assertFalse(serializeAgain.contains("\"cluster\":{\"present\":true},"));
        assertTrue(serializeAgain.contains("\"cluster\":{\"present\":false},"));
    }

    @Test
    public void testOptionalSerialization() throws JsonProcessingException {
        assertEquals("{\"present\":false}", objectMapper.writeValueAsString(Optional.empty()));
        assertEquals("{\"present\":true}", objectMapper.writeValueAsString(Optional.of("test")));
    }

    @Test
    public void testAckSerialization() throws Exception {
        Ack ack = Ack.getInstance();
        String s = Jackson.toJson(ack);
        assertEquals("{}", s);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(ack);
            out.flush();
            byte[] actual = bos.toByteArray();
            byte[] expected = {-84, -19, 0, 5, 115, 114, 0, 22, 105, 111, 46, 109, 97, 110, 116, 105, 115, 114, 120, 46, 99, 111, 109, 109, 111, 110, 46, 65, 99, 107, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 120, 112};
            assertArrayEquals(expected, actual);
        }
    }
}
