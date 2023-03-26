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

    @Deprecated
    public static MantisProperties getInstance() {
        return instance;
    }

    @Deprecated
    /**
     * Use {@link #getProperty(String)} instead.
     */
    public String getStringValue(String name) {
        if (name != null) {
            return getProperty(name, env.get(name));
        } else {
            return null;
        }

    }

    public static String getProperty(String key) {
        return getProperty(key, null);
    }

    public static String getProperty(String key, String defaultVal) {
        if (key == null) {
            return null;
        }
        String value = System.getProperty(key);
        if (value != null) {
            return value;
        }
        value = System.getenv(key);
        if (value != null) {
            return value;
        }
        return defaultVal;
    }
}
