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

package io.mantisrx.master.api.akka.route.v1;

import static org.junit.Assert.assertEquals;

import io.mantisrx.master.api.akka.route.Jackson;
import io.mantisrx.server.core.domain.ArtifactID;
import io.mantisrx.server.core.domain.JobArtifact;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.mantisrx.shaded.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.junit.Test;

public class JobArtifactSerdeTest {

    private static final ObjectMapper mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule());
    public static final SimpleFilterProvider DEFAULT_FILTER_PROVIDER;

    static {
        DEFAULT_FILTER_PROVIDER = new SimpleFilterProvider();
        DEFAULT_FILTER_PROVIDER.setFailOnUnknownId(false);
    }

    @Test
    public void testIfJobArtifactIsSerializableByJson() throws Exception {
        final JobArtifact artifact =
            JobArtifact.builder()
                .artifactID(ArtifactID.of("id"))
                .name("proj1")
                .version("v1")
                .createdAt(Instant.ofEpochMilli(1668135952L))
                .runtimeType("sbn")
                .dependencies(ImmutableMap.of("de1", "1.0.0"))
                .tags(ImmutableMap.of("jdkVersion", "9"))
                .entrypoint("entrypoint")
                .build();
        String metaJson = Jackson.toJSON(mapper, null, artifact);
        assertEquals(metaJson, "{\"artifactID\":{\"resourceID\":\"id\"},\"name\":\"proj1\",\"version\":\"v1\",\"createdAt\":1668135.952000000,\"runtimeType\":\"sbn\",\"dependencies\":{\"de1\":\"1.0.0\"},\"entrypoint\":\"entrypoint\",\"tags\":{\"jdkVersion\":\"9\"}}");

        final JobArtifact actual = Jackson.fromJSON(mapper, metaJson, JobArtifact.class);
        assertEquals(artifact, actual);
    }

    @Test
    public void testIfJobArtifactIsSerializableByJsonBackCompat() throws Exception {
        final JobArtifact artifact =
            JobArtifact.builder()
                .artifactID(ArtifactID.of("id"))
                .name("proj1")
                .version("v1")
                .createdAt(Instant.ofEpochMilli(1668135952L))
                .runtimeType("sbn")
                .dependencies(ImmutableMap.of("de1", "1.0.0"))
                .entrypoint("entrypoint")
                .build();
        String metaJson = Jackson.toJSON(mapper, null, artifact);
        String rawStr = "{\"artifactID\":{\"resourceID\":\"id\"},\"name\":\"proj1\",\"version\":\"v1\",\"createdAt\":1668135.952000000,\"runtimeType\":\"sbn\",\"dependencies\":{\"de1\":\"1.0.0\"},\"entrypoint\":\"entrypoint\",\"tags\":null}";

        assertEquals(metaJson, rawStr);

        JobArtifact actual = Jackson.fromJSON(mapper, rawStr, JobArtifact.class);
        assertEquals(artifact, actual);

        String rawStrOldVersion = "{\"artifactID\":{\"resourceID\":\"id\"},\"name\":\"proj1\",\"version\":\"v1\",\"createdAt\":1668135.952000000,\"runtimeType\":\"sbn\",\"dependencies\":{\"de1\":\"1.0.0\"},\"entrypoint\":\"entrypoint\"}";
        actual = Jackson.fromJSON(mapper, rawStrOldVersion, JobArtifact.class);
        assertEquals(artifact, actual);
    }
}
