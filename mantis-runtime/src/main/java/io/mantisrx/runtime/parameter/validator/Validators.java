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

import java.util.EnumSet;
import rx.functions.Func1;


public class Validators {

    private Validators() {}

    public static <T extends Number> Validator<T> range(final Number start, final Number end) {
        Func1<T, Validation> func = new Func1<T, Validation>() {
            @Override
            public Validation call(Number t1) {
                if (t1.doubleValue() >= start.doubleValue()
                        && t1.doubleValue() <= end.doubleValue()) {
                    return Validation.passed();
                } else {
                    return Validation.failed("range must be between"
                            + " " + start + " and " + end);
                }
            }
        };
        return new Validator<>("range >=" + start + "<=" + end, func);
    }

    public static Validator<String> notNullOrEmpty() {
        Func1<String, Validation> func = new Func1<String, Validation>() {
            @Override
            public Validation call(String t1) {
                if (t1 == null || t1.length() == 0) {
                    return Validation.failed("string must not be null or empty");
                } else {
                    return Validation.passed();
                }
            }
        };
        return new Validator<>("not null or empty", func);
    }

    public static <T> Validator<T> alwaysPass() {
        Func1<T, Validation> func = new Func1<T, Validation>() {
            @Override
            public Validation call(T t1) {
                return Validation.passed();
            }
        };
        return new Validator<>("always passes validation", func);
    }

    public static <T extends Enum<T>> Validator<EnumSet<T>> notNullOrEmptyEnumCSV() {
        Func1<EnumSet<T>, Validation> func = new Func1<EnumSet<T>, Validation>() {
            @Override
            public Validation call(EnumSet<T> t1) {
                if (t1.isEmpty()) {
                    return Validation.failed("enum constant csv list must not be null or empty");
                } else {
                    return Validation.passed();
                }
            }
        };
        return new Validator<>("not null or empty", func);
    }
}
