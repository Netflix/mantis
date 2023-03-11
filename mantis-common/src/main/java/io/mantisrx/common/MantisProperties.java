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

package io.mantisrx.common;

import java.util.Map;


public class MantisProperties {

    private static final MantisProperties instance = new MantisProperties();
    private Map<String, String> env;

    private MantisProperties() {
        env = System.getenv();
    }

    public static MantisProperties getInstance() {
        return instance;
    }

    public String getStringValue(String name) {
        if (name != null && env.containsKey(name)) {
            return System.getProperty(name, env.get(name));
        } else {
            return null;
        }

    }

    public static String getProperty(String key) {
        return System.getProperty(key, System.getenv(key));
    }

    public static String getProperty(String key, String defaultVal) {
        return System.getProperty(key, defaultVal);
    }
}
