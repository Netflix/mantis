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

package io.mantisrx.runtime.parameter.validator;

import rx.functions.Func1;


public class Validator<T> {

    private final String description;
    private final Func1<T, Validation> validator;

    public Validator(String description, Func1<T, Validation> validator) {
        this.description = description;
        this.validator = validator;
    }

    public String getDescription() {
        return description;
    }

    public Func1<T, Validation> getValidator() {
        return validator;
    }

    @Override
    public String toString() {
        return "Validator [description=" + description + "]";
    }
}
