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

package io.mantisrx.runtime.parameter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ParametersTest {
    private Parameters parameters;

    @BeforeEach
    public void setup() {
        final Set<String> requiredParameters = new HashSet<>();
        final Set<String> parameterDefinitions = new HashSet<>();
        final Map<String, Object> state = new HashMap<>();

        // required parameter r1 -> v1
        requiredParameters.add("r1");
        parameterDefinitions.add("r1");
        state.put("r1", "v1");

        // required parameter r2 -> N/A
        requiredParameters.add("r2");
        parameterDefinitions.add("r2");

        // optional parameter o1 -> v1
        parameterDefinitions.add("o1");
        state.put("o1", "v1");

        // optional parameter o2 -> null
        parameterDefinitions.add("o2");
        state.put("o2", null);

        parameters = new Parameters(state, requiredParameters, parameterDefinitions);
    }

    @Test
    public void testGet() {
        // Get required parameter r1 should succeed
        assertEquals("v1", parameters.get("r1"));
        // Get required parameter r2 should fail because it does not have a value
        assertThrows(ParameterException.class, () -> parameters.get("r2"));
        // Get optional parameter o1 should succeed
        assertEquals("v1", parameters.get("o1"));
        // Get optional parameter o2 should succeed with null value
        assertNull(parameters.get("o2"));
        // Get undefined parameter u1 should fail
        assertThrows(ParameterException.class, () -> parameters.get("u1"));
    }

    @Test
    public void testGetWithDefaultValue() {
        // Get required parameter r1 should return value in state
        assertEquals("v1", parameters.get("r1", "defaultValue"));
        // Get required parameter r2 should return the provided default value
        assertEquals("defaultValue", parameters.get("r2", "defaultValue"));
        // Get optional parameter o1 should return value in state
        assertEquals("v1", parameters.get("o1", "defaultValue"));
        // Get optional parameter o2 should return the provided default value instead of null
        assertEquals("defaultValue", parameters.get("o2", "defaultValue"));
        // Get undefined parameter u1 should return the provided default value
        assertEquals("defaultValue", parameters.get("u2", "defaultValue"));
    }

    static <T extends Throwable> void assertThrows(Class<T> expectedType, Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable t) {
            if (expectedType.isInstance(t)) {
                return;
            }
        }
        fail("Should have failed with exception class " + expectedType);
    }
}
