package io.mantisrx.runtime.common.compression;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.mantisrx.runtime.common.MantisServerSentEvent;
import org.junit.Assert;
import org.junit.Test;


public class CompressionUtilsTest {

    @Test public void shouldTokenizeWithEventsContainingPartialDelimiterMatches() {
        String testInput = "ab$cdef$$$ghi$jkl$$$lmno$$pqrst$";
        BufferedReader reader = new BufferedReader(new StringReader(testInput));
        try {
            List<MantisServerSentEvent> result = CompressionUtils.tokenize(reader);

            assertEquals(result.size(), 3);
            assertEquals(result.get(0).getEventAsString(), "ab$cdef");
            assertEquals(result.get(1).getEventAsString(), "ghi$jkl");
            assertEquals(result.get(2).getEventAsString(), "lmno$$pqrst$");

        } catch (IOException ex) {
            Assert.fail("Tokenization threw an IO exception that was unexpected");
        }
    }

    @Test public void shouldTokenizeWithEventsContainingPartialDelimiterMatchesWithCustomDelimiter() {
        String delimiter = UUID.randomUUID().toString();

        String event1 = "ab" + delimiter.substring(0, 9) + "cdef";
        String event2 = "ghi" + delimiter.substring(0, 5) + "jkl";
        String event3 = "lmno" + delimiter.substring(0, 4) + "pqrst" + delimiter.substring(0, 2);
        String testInput = event1
                + delimiter
                + event2
                + delimiter
                + event3;
        BufferedReader reader = new BufferedReader(new StringReader(testInput));
        try {
            List<MantisServerSentEvent> result = CompressionUtils.tokenize(reader, delimiter);

            assertEquals(result.size(), 3);
            assertEquals(result.get(0).getEventAsString(), event1);
            assertEquals(result.get(1).getEventAsString(), event2);
            assertEquals(result.get(2).getEventAsString(), event3);

        } catch (IOException ex) {
            Assert.fail("Tokenization threw an IO exception that was unexpected");
        }
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
