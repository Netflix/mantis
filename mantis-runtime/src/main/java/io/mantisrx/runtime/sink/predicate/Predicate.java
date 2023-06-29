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

package io.mantisrx.runtime.sink.predicate;

import java.util.List;
import java.util.Map;
import rx.functions.Func1;


public class Predicate<T> {

    private final String description;
    private final Func1<Map<String, List<String>>, Func1<T, Boolean>> predicate;

    public Predicate(String description,
                     Func1<Map<String, List<String>>, Func1<T, Boolean>> predicate) {
        this.description = description;
        this.predicate = predicate;
    }

    public String getDescription() {
        return description;
    }

    public Func1<Map<String, List<String>>, Func1<T, Boolean>> getPredicate() {
        return predicate;
    }
}
