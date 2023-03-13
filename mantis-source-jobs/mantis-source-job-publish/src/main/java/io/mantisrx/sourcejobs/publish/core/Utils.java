/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.sourcejobs.publish.core;

import io.mantisrx.common.MantisProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Utils {

    public static String getEnvVariable(String envVariableName, String defaultValue) {

        String v = MantisProperties.getProperty(envVariableName);
        if (v != null && !v.isEmpty()) {
            return v;
        }
        return defaultValue;

    }

    public static List<String> convertCommaSeparatedStringToList(String filterBy) {
        List<String> terms = new ArrayList<>();
        if (filterBy != null && !filterBy.isEmpty()) {
            terms = Arrays.asList(filterBy.split("\\s*,\\s*"));
        }

        return terms;
    }


}
