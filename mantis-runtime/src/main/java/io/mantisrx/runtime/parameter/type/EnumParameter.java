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

package io.mantisrx.runtime.parameter.type;

import io.mantisrx.runtime.parameter.ParameterDecoder;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class EnumParameter<T extends Enum<T>> extends ParameterDefinition.Builder<Enum<T>> {

    private final Class<T> clazz;

    public EnumParameter(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public ParameterDecoder<Enum<T>> decoder() {
        return value -> T.valueOf(clazz, value.trim());
    }

    @Override
    public String getTypeDescription() {
        List<String> ts = Arrays.stream(clazz.getEnumConstants()).map(Enum::name).collect(Collectors.toList());
        return "One of (" + String.join(",", ts) + ")";
    }

    @Override
    public Class<Enum<T>> classType() {
        return null;
    }
}
