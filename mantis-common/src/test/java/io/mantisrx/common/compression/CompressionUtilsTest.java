/*
 * Copyright 2021 Netflix, Inc.
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
package io.mantisrx.common.compression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.mantisrx.common.MantisServerSentEvent;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class CompressionUtilsTest {

    @Test
    public void shouldTokenizeWithEventsContainingPartialDelimiterMatches() throws Exception {
        String testInput = "ab$cdef$$$ghi$jkl$$$lmno$$pqrst$";
        BufferedReader reader = new BufferedReader(new StringReader(testInput));
        List<MantisServerSentEvent> result = CompressionUtils.tokenize(reader);

        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getEventAsString(), "ab$cdef");
        assertEquals(result.get(1).getEventAsString(), "ghi$jkl");
        assertEquals(result.get(2).getEventAsString(), "lmno$$pqrst$");
    }

    @Test
    public void shouldTokenizeWithEventsContainingPartialDelimiterMatchesWithCustomDelimiter() throws Exception {
        String delimiter = "a04f0418-bdff-4f53-af7d-9f5a093b9d65";

        String event1 = "ab" + delimiter.substring(0, 9) + "cdef";
        String event2 = "ghi" + delimiter.substring(0, 5) + "jkl";
        String event3 = "lmno" + delimiter.substring(0, 4) + "pqrst" + delimiter.substring(0, 2);
        String testInput = event1
                + delimiter
                + event2
                + delimiter
                + event3;
        BufferedReader reader = new BufferedReader(new StringReader(testInput));
        List<MantisServerSentEvent> result = CompressionUtils.tokenize(reader, delimiter);

        List<String> actual = result.stream().map(e -> e.getEventAsString()).collect(Collectors.toList());
        assertEquals(Arrays.asList(event1,event2,event3), actual, "Delimiter: '" + delimiter + "'");
    }

    @Test
    public void testDelimiterWiithPrefixMatchingEndOfMEssage() throws Exception {
        // Delimiter starts with 'c', event1 ends with 'c'
        String delimiter = "ccd";

        String event1 = "abc";
        String event2 = "def";
        String event3 = "ghi";
        String testInput = event1
                + delimiter
                + event2
                + delimiter
                + event3;
        BufferedReader reader = new BufferedReader(new StringReader(testInput));
        List<MantisServerSentEvent> result = CompressionUtils.tokenize(reader, delimiter);

        List<String> actual = result.stream().map(e -> e.getEventAsString()).collect(Collectors.toList());
        assertEquals(Arrays.asList(event1,event2,event3), actual, "Delimiter: '" + delimiter + "'");
    }

    @Test
    public void testMultiline() throws Exception {
        String delimiter = "ccd";

        String event1 = "abc";
        String event2 = "def";
        String event3 = "ghi";
        StringBuffer buf = new StringBuffer();
        String testInput = event1
                + delimiter
                + event2
                + delimiter
                + event3;
        // Turn input into 1 character per line
        for (int i = 0; i < testInput.length(); i++) {
            buf.append(testInput.charAt(i)).append("\n");
        }
        testInput = buf.toString();

        BufferedReader reader = new BufferedReader(new StringReader(testInput));
        List<MantisServerSentEvent> result = CompressionUtils.tokenize(reader, delimiter);

        List<String> actual = result.stream().map(e -> e.getEventAsString()).collect(Collectors.toList());
        assertEquals(Arrays.asList(event1,event2,event3), actual, "Delimiter: '" + delimiter + "'");
    }

    @Test
    public void testCompression() throws Exception {
        List<byte[]> events1 = new ArrayList<>();
        events1.add("1".getBytes());
        events1.add("2".getBytes());
        events1.add("3".getBytes());
        List<byte[]> events2 = new ArrayList<>();
        events2.add("4".getBytes());
        events2.add("5".getBytes());
        events2.add("6".getBytes());
        List<List<byte[]>> buffer = new ArrayList<>();
        buffer.add(events1);
        buffer.add(events2);

        byte[] compressed = CompressionUtils.compressAndBase64EncodeBytes(buffer, false);
        List<MantisServerSentEvent> decompressed = CompressionUtils.decompressAndBase64Decode(new String(compressed), true, false);
        assertEquals("1", decompressed.get(0).getEventAsString());
        assertEquals("2", decompressed.get(1).getEventAsString());
        assertEquals("3", decompressed.get(2).getEventAsString());
        assertEquals("4", decompressed.get(3).getEventAsString());
        assertEquals("5", decompressed.get(4).getEventAsString());
        assertEquals("6", decompressed.get(5).getEventAsString());

        // test snappy
        compressed = CompressionUtils.compressAndBase64EncodeBytes(buffer, true);
        decompressed = CompressionUtils.decompressAndBase64Decode(new String(compressed), true, true);
        assertEquals("1", decompressed.get(0).getEventAsString());
        assertEquals("2", decompressed.get(1).getEventAsString());
        assertEquals("3", decompressed.get(2).getEventAsString());
        assertEquals("4", decompressed.get(3).getEventAsString());
        assertEquals("5", decompressed.get(4).getEventAsString());
        assertEquals("6", decompressed.get(5).getEventAsString());

        // test custom delimiter
        compressed = CompressionUtils.compressAndBase64EncodeBytes(buffer, true, "abcdefg".getBytes());
        decompressed = CompressionUtils.decompressAndBase64Decode(new String(compressed), true, true, "abcdefg");
        assertEquals("1", decompressed.get(0).getEventAsString());
        assertEquals("2", decompressed.get(1).getEventAsString());
        assertEquals("3", decompressed.get(2).getEventAsString());
        assertEquals("4", decompressed.get(3).getEventAsString());
        assertEquals("5", decompressed.get(4).getEventAsString());
        assertEquals("6", decompressed.get(5).getEventAsString());
    }
}
