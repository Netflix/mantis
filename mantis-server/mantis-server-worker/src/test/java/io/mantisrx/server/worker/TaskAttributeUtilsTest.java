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

package io.mantisrx.server.worker;

import static org.junit.Assert.assertEquals;

import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class TaskAttributeUtilsTest {

    @Test
    public void testTaskAttibuteUtils() {
        String t1 = "";
        Map<String, String> res1 = TaskAttributeUtils.getTaskExecutorAttributes(t1);
        assertEquals(ImmutableMap.of(), res1);

        String t2 = "key1:val1";
        Map<String, String> res2 = TaskAttributeUtils.getTaskExecutorAttributes(t2);
        assertEquals(ImmutableMap.of("key1", "val1"), res2);

        String t3 = "key1:val1,key2:val2,key3:val3";
        Map<String, String> res3 = TaskAttributeUtils.getTaskExecutorAttributes(t3);
        assertEquals(ImmutableMap.of("key1", "val1", "key2", "val2", "key3", "val3"), res3);
    }
}
