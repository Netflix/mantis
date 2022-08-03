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

import io.mantisrx.common.WorkerConstants;
import io.mantisrx.shaded.com.google.common.base.Splitter;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;

public class TaskAttributeUtils {
    public static Map<String, String> getTaskExecutorAttributes(String input) {
        if (input == null || input.isEmpty()) {
            return ImmutableMap.of();
        }

        return Splitter.on(",").withKeyValueSeparator(':').split(input);
    }

    /**
     * Convert system env attribute (e.g. "k1:v1,k2:v2") to map.
     */
    public static Map<String, String> getTaskExecutorAttributes() {
        String envStr = System.getenv(WorkerConstants.WORKER_TASK_ATTRIBUTE_ENV_KEY);
        if (envStr == null || envStr.isEmpty()) {
            return ImmutableMap.of();
        }

        return getTaskExecutorAttributes(envStr);
    }
}
