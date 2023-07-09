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

package io.mantisrx.runtime.descriptor;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;


public class ParameterInfo {

    private final String name;
    private final String description;
    private final String defaultValue;
    private final String parameterType;
    private final String validatorDescription;
    private final boolean required;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public ParameterInfo(@JsonProperty("name") String name,
                         @JsonProperty("description") String description,
                         @JsonProperty("defaultValue") String defaultValue,
                         @JsonProperty("parmaterType") String parameterType,
                         @JsonProperty("validatorDescription") String validatorDescription,
                         @JsonProperty("required") boolean required) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.parameterType = parameterType;
        this.validatorDescription = validatorDescription;
        this.required = required;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getParameterType() {
        return parameterType;
    }

    public String getValidatorDescription() {
        return validatorDescription;
    }

    public boolean isRequired() {
        return required;
    }

    @Override
    public String toString() {
        return "ParameterInfo{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", defaultValue='" + defaultValue + '\'' +
                ", parameterType='" + parameterType + '\'' +
                ", validatorDescription='" + validatorDescription + '\'' +
                ", required=" + required +
                '}';
    }
}
