/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.runtime.parameter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SourceJobParametersTest {
    @Test
    public void shouldParseTargetInfoJson() {
        String json = "{\"targets\":[" +
                "{" +
                "  \"sourceJobName\":\"TestSource\"," +
                "  \"criterion\":\"select * from stream\"," +
                "  \"unknownProperty\":\"value\"" +
                "}," +
                "{" +
                "  \"sourceJobName\":\"TestSource2\"," +
                "  \"criterion\":\"select * from stream2\"," +
                "  \"clientId\":\"TestClientId2\"," +
                "  \"sample\":10," +
                "  \"isBroadcastMode\":true," +
                "  \"enableMetaMessages\":true," +
                "  \"mantis.EnableCompressedBinary\":true," +
                "  \"enableMetaMessages\":true," +
                "  \"mantis.CompressionDelimiter\":\"Delimiter2\"" +
                "}" +
                "]}";
        List<SourceJobParameters.TargetInfo> infos = SourceJobParameters.parseTargetInfo(json);
        List<SourceJobParameters.TargetInfo> expected = ImmutableList.of(
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from stream").build(),
                new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource2").withQuery("select * from stream2")
                        .withClientId("TestClientId2").withSamplePerSec(10).withBroadCastMode().withMetaMessagesEnabled().withBinaryCompressionEnabled().withDelimiter("Delimiter2").build()
        );
        assertEquals(expected, infos);
    }

    @Test
    public void shouldParseEmptyJson() {
        String json = "{}";
        List<SourceJobParameters.TargetInfo> infos = SourceJobParameters.parseTargetInfo(json);
        assertEquals(0, infos.size());

        json = "invalid_json";
        infos = SourceJobParameters.parseTargetInfo(json);
        assertEquals(0, infos.size());
    }

    @Test
    public void shouldInsertDefaultClientIdIfNoneIsPresent() {

        SourceJobParameters.TargetInfo target = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from stream").build();

        List<SourceJobParameters.TargetInfo> result = SourceJobParameters
                .enforceClientIdConsistency(Collections.singletonList(target), "defaultId");

        SourceJobParameters.TargetInfo firstResult = result.get(0);
        assertEquals(firstResult.clientId, "defaultId");
    }

    @Test
    public void shouldNotChangeSingleSourceWithClientId() {

        SourceJobParameters.TargetInfo target = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from stream").withClientId("myClient").build();

        List<SourceJobParameters.TargetInfo> result = SourceJobParameters
                .enforceClientIdConsistency(Collections.singletonList(target), "defaultId");

        SourceJobParameters.TargetInfo firstResult = result.get(0);
        assertEquals(firstResult.clientId, "myClient");
    }

    @Test
    public void shouldChangeSecondTargetId() {

        SourceJobParameters.TargetInfo target = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from stream").withClientId("myClient").build();

        SourceJobParameters.TargetInfo target2 = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from stream").withClientId("myClient").build();

        List<SourceJobParameters.TargetInfo> result = SourceJobParameters
                .enforceClientIdConsistency(Lists.newArrayList(target, target2), "defaultId");

        assertEquals("myClient", result.get(0).clientId);
        assertEquals("myClient_1", result.get(1).clientId);
    }

    @Test
    public void shouldChangeSecondTargetIdWithDefaults() {

        SourceJobParameters.TargetInfo target = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from stream").build();

        SourceJobParameters.TargetInfo target2 = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from stream").build();

        List<SourceJobParameters.TargetInfo> result = SourceJobParameters
                .enforceClientIdConsistency(Lists.newArrayList(target, target2), "defaultId");

        assertEquals("defaultId", result.get(0).clientId);
        assertEquals("defaultId_1", result.get(1).clientId);
    }

    @Test
    public void shouldNotImpactUnrelatedSource() {

        SourceJobParameters.TargetInfo target = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from stream").withClientId("myClient").build();

        SourceJobParameters.TargetInfo target2 = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("TestSource").withQuery("select * from streamz").withClientId("myClient").build();

        SourceJobParameters.TargetInfo target3 = new SourceJobParameters.TargetInfoBuilder().withSourceJobName("UnrelatedSource").withQuery("select * from streamzz").withClientId("myUnrelatedClient").build();

        List<SourceJobParameters.TargetInfo> result = SourceJobParameters
                .enforceClientIdConsistency(Lists.newArrayList(target, target2, target3), "defaultId");

        assertEquals("myClient", result.get(0).clientId);
        assertEquals("myClient_1", result.get(1).clientId);
        assertEquals("myUnrelatedClient", result.get(2).clientId);
    }
}
