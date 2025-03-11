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
import io.mantisrx.master.api.akka.payloads.PayloadUtils;
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

import io.mantisrx.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.Test;


public class JacksonTest {
    private static final ObjectMapper objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        .registerModule(new Jdk8Module());

    @Test
    public void testDeser4() throws IOException {
        final String jsonStr =
            PayloadUtils.getStringFromResource("jackson/JobMetadata1.json");

        final List<MantisJobMetadataView> jobIdInfos = Jackson.fromJSON(objectMapper, jsonStr, new TypeReference<List<MantisJobMetadataView>>() { });
        assertEquals(1, jobIdInfos.size());
        final MantisJobMetadataView jobInfo = jobIdInfos.get(0);
        assertEquals("sine-function-1", jobInfo.getJobMetadata().getJobId());
        assertEquals(2, jobInfo.getWorkerMetadataList().size());
        assertEquals(2, jobInfo.getStageMetadataList().size());
        assertEquals("mantisagent", jobInfo.getWorkerMetadataList().get(0).getResourceCluster().get().getResourceID());

        MantisWorkerMetadataWritable mwm = jobInfo.getWorkerMetadataList().get(0);
        mwm.setCluster(Optional.of("test"));

        final String out = objectMapper.writer(Jackson.DEFAULT_FILTER_PROVIDER).writeValueAsString(mwm);
        assertFalse(out.contains("\"cluster\":{\"present\":true},"));
        final String serializeAgain = objectMapper.writeValueAsString(objectMapper.readValue(out, MantisWorkerMetadataWritable.class));
        assertFalse(out.contains("{\"present\":"));
        assertTrue(serializeAgain.contains("\"resourceCluster\":{\"resourceID\":\"mantisagent\"}"));
    }

    @Test
    public void testOptionalSerialization() throws JsonProcessingException {
        assertEquals("null", objectMapper.writeValueAsString(Optional.empty()));
        assertEquals("\"test\"", objectMapper.writeValueAsString(Optional.of("test")));
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
