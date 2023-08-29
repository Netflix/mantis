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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;


/**
 * @author hrangarajan@netflix.com
 */
public class EnumCSVParameter<T extends Enum<T>> extends ParameterDefinition.Builder<EnumSet<T>> {

    private final Class<T> clazz;

    public EnumCSVParameter(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public ParameterDecoder<EnumSet<T>> decoder() {
        return new ParameterDecoder<EnumSet<T>>() {
            @Override
            public EnumSet<T> decode(String value) {
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
                set.addAll(enumVals);
                return set;
            }
        };
    }

    @Override
    public String getTypeDescription() {
        List<String> ts = Arrays.stream(clazz.getEnumConstants()).map(Enum::name).collect(Collectors.toList());
        return "Comma separated set of values: " + String.join(",", ts);
    }

    @Override
    public Class<EnumSet<T>> classType() {
        return null;
    }
}
