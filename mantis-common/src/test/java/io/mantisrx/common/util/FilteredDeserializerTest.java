/*
 * Copyright 2026 Netflix, Inc.
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

package io.mantisrx.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;

public class FilteredDeserializerTest {

    private static final AtomicBoolean gadgetDeserialized = new AtomicBoolean();

    static class Gadget implements Serializable {
        private static final long serialVersionUID = 1L;

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            gadgetDeserialized.set(true);
        }
    }

    static class GadgetCarryingException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final Gadget payload = new Gadget();
    }

    @Before
    public void resetGadgetFlag() {
        gadgetDeserialized.set(false);
    }

    @Test
    public void readsHashMapOfStrings() throws IOException {
        Map<String, String> original = new HashMap<>();
        original.put("key", "value");

        assertEquals(original, FilteredDeserializer.readStringMap(serialize(original)));
    }

    @Test
    public void readsLinkedHashMapOfStrings() throws IOException {
        Map<String, String> original = new LinkedHashMap<>();
        original.put("a", "1");
        original.put("b", "2");

        assertEquals(original, FilteredDeserializer.readStringMap(serialize(original)));
    }

    @Test
    public void rejectsNonMapPayloadForStringMap() throws IOException {
        assertRejectedAsInvalidClass(() -> FilteredDeserializer.readStringMap(serialize(new Gadget())));
        assertTrue(!gadgetDeserialized.get());
    }

    @Test
    public void rejectsGadgetNestedInMapValue() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("key", new Gadget());

        assertRejectedAsInvalidClass(() -> FilteredDeserializer.readStringMap(serialize(map)));
        assertTrue(!gadgetDeserialized.get());
    }

    @Test
    public void rejectsNonMapSerializableForStringMap() throws IOException {
        assertRejectedAsInvalidClass(() -> FilteredDeserializer.readStringMap(serialize(new ArrayList<>())));
    }

    @Test
    public void readsThrowableWithCauseAndStackTrace() throws IOException {
        Throwable original = new IllegalStateException("outer", new IOException("inner"));

        Throwable read = FilteredDeserializer.readThrowable(serialize(original));

        assertEquals(IllegalStateException.class, read.getClass());
        assertEquals("outer", read.getMessage());
        assertEquals("inner", read.getCause().getMessage());
        assertEquals(original.getStackTrace().length, read.getStackTrace().length);
    }

    @Test
    public void readsThrowableWithSuppressedExceptions() throws IOException {
        Throwable original = new RuntimeException("outer");
        original.addSuppressed(new IOException("suppressed"));

        Throwable read = FilteredDeserializer.readThrowable(serialize(original));

        assertEquals("suppressed", read.getSuppressed()[0].getMessage());
    }

    @Test
    public void rejectsNonThrowablePayloadForThrowable() throws IOException {
        assertRejectedAsInvalidClass(() -> FilteredDeserializer.readThrowable(serialize(new Gadget())));
        assertTrue(!gadgetDeserialized.get());
    }

    @Test
    public void rejectsGadgetNestedInThrowable() throws IOException {
        assertRejectedAsInvalidClass(() -> FilteredDeserializer.readThrowable(serialize(new GadgetCarryingException())));
        assertTrue(!gadgetDeserialized.get());
    }

    @Test
    public void rejectsThrowablePayloadForStringMap() throws IOException {
        assertRejectedAsInvalidClass(() -> FilteredDeserializer.readStringMap(serialize(new RuntimeException("x"))));
    }

    private interface Deserialization {
        void run() throws IOException;
    }

    private static byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(object);
        }
        return bytes.toByteArray();
    }

    private static void assertRejectedAsInvalidClass(Deserialization deserialization) throws IOException {
        try {
            deserialization.run();
            fail("expected deserialization to be rejected");
        } catch (RuntimeException e) {
            assertTrue("unexpected cause: " + e.getCause(), e.getCause() instanceof InvalidClassException);
        }
    }
}
