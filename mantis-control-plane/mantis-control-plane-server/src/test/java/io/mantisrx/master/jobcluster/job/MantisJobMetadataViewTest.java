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

package io.mantisrx.master.jobcluster.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.mantisrx.master.api.akka.payloads.PayloadUtils;
import io.mantisrx.server.master.domain.DataFormatAdapter;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MantisJobMetadataViewTest {
    private static final Logger logger = LoggerFactory.getLogger(MantisJobMetadataViewTest.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .registerModule(new Jdk8Module());
    }

    @Test
    public void testJsonReadWrite() throws Exception {
        String metadata = PayloadUtils.getStringFromResource("persistence/job_metadata_view.json");
        MantisJobMetadataView metadataView = mapper.readValue(metadata, MantisJobMetadataView.class);

        SimpleFilterProvider filterProvider = new SimpleFilterProvider();
        filterProvider.setFailOnUnknownId(false);
        String output = mapper.writer(filterProvider).writeValueAsString(metadataView);
        assertEquals(metadata.replaceAll("\\s", ""), output.replace("\"terminatedAt\":-1,", ""));
    }

    @Test
    public void testJsonWithTerminatedAt() throws Exception {
        String metadata = PayloadUtils.getStringFromResource("persistence/job_metadata_view.json");
        MantisJobMetadataView metadataView = mapper.readValue(metadata, MantisJobMetadataView.class);

        FilterableMantisJobMetadataWritable jobMetadata =
            (FilterableMantisJobMetadataWritable) metadataView.getJobMetadata();

        jobMetadata.addJobStageIfAbsent(metadataView.getStageMetadataList().get(0));

        IMantisJobMetadata iJobMetadata = DataFormatAdapter.convertMantisJobWriteableToMantisJobMetadata(jobMetadata,
            null);

        MantisJobMetadataView metadataViewWithTerminate = new MantisJobMetadataView(iJobMetadata, 999,
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false);

        SimpleFilterProvider filterProvider = new SimpleFilterProvider();
        filterProvider.setFailOnUnknownId(false);
        String output = mapper.writer(filterProvider).writeValueAsString(metadataViewWithTerminate);

        assertTrue(output.indexOf("\"terminatedAt\":999") > 0);
    }
}
