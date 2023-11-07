/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.agent.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DurableBooleanStateTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();


    @Test
    public void testFunctionality() throws Exception {
        File tempFile = tempFolder.newFile("myTempFile.txt");
        String absolutePath = tempFile.getAbsolutePath();
        DurableBooleanState durableBooleanState = new DurableBooleanState(absolutePath);
        assertFalse(durableBooleanState.getState());
        durableBooleanState.setState(true);
        boolean state = durableBooleanState.getState();
        assertTrue(state);

        durableBooleanState.setState(false);
        assertFalse(durableBooleanState.getState());

        durableBooleanState.setState(true);
        assertTrue(durableBooleanState.getState());


        DurableBooleanState anotherBooleanState = new DurableBooleanState(absolutePath);
        assertTrue(anotherBooleanState.getState());
    }
}
