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


package io.reactivex.mantis.remote.observable;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class RemoteObservableErrorDeserializationTest {

    @Test
    public void rejectsNonThrowableErrorPayload() throws Exception {
        byte[] outOfAllowlist = serialize(new java.util.concurrent.atomic.AtomicInteger(1));

        try {
            RemoteObservable.fromBytesToThrowable(outOfAllowlist);
            fail("expected deserialization to be rejected");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof java.io.InvalidClassException);
        }
    }

    private static byte[] serialize(Object object) throws java.io.IOException {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
            out.writeObject(object);
        }
        return bytes.toByteArray();
    }
}
