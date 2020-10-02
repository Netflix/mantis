/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.publish.core;

import java.util.Arrays;
import java.util.List;


public class ObjectUtils {

    public static void checkNotNull(String paramName, String param) {
        if (param == null || param.isEmpty()) {
            throw new IllegalArgumentException(paramName + " cannot be null");
        }
    }

    public static void checkArgCondition(String paramName, boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException(paramName + " is invalid");
        }
    }

    public static List<String> convertCommaSeparatedStringToList(String str) {
        return Arrays.asList(str.trim().split("\\,"));
    }
}
