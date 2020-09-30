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

package com.netflix.mantis.examples.config;

import java.util.Map;

import io.mantisrx.common.codec.Codecs;
import io.mantisrx.runtime.KeyToScalar;
import io.mantisrx.runtime.ScalarToKey;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.codec.JacksonCodecs;


public class StageConfigs {

    public static ScalarToScalar.Config<String, String> scalarToScalarConfig() {
        return new ScalarToScalar.Config<String, String>()
                .codec(Codecs.string());
    }

    public static KeyToScalar.Config<String, Map<String, Object>, String> keyToScalarConfig() {
        return new KeyToScalar.Config<String, Map<String, Object>, String>()
                .description("sum events ")
                .keyExpireTimeSeconds(10)
                .codec(Codecs.string());
    }

    public static ScalarToKey.Config<String, String, Map<String, Object>> scalarToKeyConfig() {
        return new ScalarToKey.Config<String, String, Map<String, Object>>()
                .description("Group event data by ip")
                .concurrentInput()
                .keyExpireTimeSeconds(1)
                .codec(JacksonCodecs.mapStringObject());
    }
}
