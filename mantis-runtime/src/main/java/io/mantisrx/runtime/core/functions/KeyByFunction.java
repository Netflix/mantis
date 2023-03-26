/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.runtime.core.functions;

/**
 * Functional interface for extracting a key of type {@code K} from an input value
 * of type {@code IN}.
 */
@FunctionalInterface
public interface KeyByFunction<K, IN> extends MantisFunction {
    /**
     * Extracts a key from the given input value.
     *
     * @param in the input value
     * @return the extracted key
     */
    K getKey(IN in);
}
