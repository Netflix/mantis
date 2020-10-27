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

package io.mantisrx.runtime.api.parameter;

import io.mantisrx.runtime.api.parameter.validator.Validator;

public class ParameterDefinition<T> {

    private String name;
    private String description;
    private T defaultValue;
    private String typeDescription;
    private Validator<? super T> validator;
    private boolean required;
    private ParameterDecoder<T> decoder;

    ParameterDefinition(Builder<T> builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.validator = builder.validator;
        this.required = builder.required;
        this.typeDescription = builder.getTypeDescription();
        this.defaultValue = builder.defaultValue;
        this.decoder = builder.decoder();
    }

    public ParameterDecoder<T> getDecoder() {
        return decoder;
    }

    public String getTypeDescription() {
        return typeDescription;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Validator<? super T> getValidator() {
        return validator;
    }

    public boolean isRequired() {
        return required;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ParameterDefinition{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", defaultValue=").append(defaultValue);
        sb.append(", typeDescription='").append(typeDescription).append('\'');
        sb.append(", validator=").append(validator);
        sb.append(", required=").append(required);
        sb.append(", decoder=").append(decoder);
        sb.append('}');
        return sb.toString();
    }

    public abstract static class Builder<T> {

        protected String name;
        protected String description;
        protected T defaultValue;
        protected Validator<? super T> validator;
        protected boolean required = false;

        public abstract ParameterDecoder<T> decoder();

        public abstract String getTypeDescription();

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> defaultValue(T defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        public Builder<T> validator(Validator<? super T> validator) {
            this.validator = validator;
            return this;
        }

        public Builder<T> required() {
            this.required = true;
            return this;
        }

        public ParameterDefinition<T> build() {
            if (validator == null) {
                throw new ParameterException("A validator must be specified for parameter: " + name);
            }
            return new ParameterDefinition<>(this);
        }
    }
}
