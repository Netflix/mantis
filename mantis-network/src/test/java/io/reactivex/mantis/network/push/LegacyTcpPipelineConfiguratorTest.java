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


package io.reactivex.mantis.network.push;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

public class LegacyTcpPipelineConfiguratorTest {

    @Test
    public void rejectsNonMapSubscribeParameters() throws Exception {
        byte[] outOfAllowlist = serialize(new java.util.concurrent.atomic.AtomicInteger(1));

        try {
            LegacyTcpPipelineConfigurator.fromBytesToMap(outOfAllowlist);
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
