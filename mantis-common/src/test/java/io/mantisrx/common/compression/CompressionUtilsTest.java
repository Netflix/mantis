package io.mantisrx.common.compression;

import io.mantisrx.common.MantisServerSentEvent;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

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
}
