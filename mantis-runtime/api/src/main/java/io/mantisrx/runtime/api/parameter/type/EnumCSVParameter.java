/*
 *
 * Copyright 2020 Netflix, Inc.
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
 *
 */

package io.mantisrx.runtime.api.parameter.type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import io.mantisrx.runtime.api.parameter.ParameterDecoder;
import io.mantisrx.runtime.api.parameter.ParameterDefinition;

public class EnumCSVParameter<T extends Enum<T>> extends ParameterDefinition.Builder<EnumSet<T>> {

    private final Class<T> clazz;

    public EnumCSVParameter(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public ParameterDecoder<EnumSet<T>> decoder() {
        return value -> {
            String[] values = value.split(",");

            if (values.length == 0) {
                return EnumSet.noneOf(clazz);
            }

            List<T> enumVals = new ArrayList<>();
            for (String val : values) {
                String trim = val.trim();
                if (trim.length() > 0) {
                    enumVals.add(T.valueOf(clazz, trim));
                }
            }

            EnumSet<T> set = EnumSet.noneOf(clazz);
            for (T val : enumVals) {
                set.add(val);
            }
            return set;
        };
    }

    @Override
    public String getTypeDescription() {
        List<String> ts = Arrays.stream(clazz.getEnumConstants()).map(x -> x.name()).collect(Collectors.toList());
        return "Comma separated set of values: " + String.join(",", ts);
    }

}
