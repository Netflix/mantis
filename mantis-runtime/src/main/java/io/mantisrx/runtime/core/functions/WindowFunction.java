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

import io.mantisrx.runtime.core.WindowSpec;
import lombok.Getter;

/**
 * A function that defines a window for grouping and processing elements of type
 * {@code IN}. The window is defined by a {@link WindowSpec} object.
 */
public class WindowFunction<IN> implements MantisFunction {

    /**
     * The window specification defining window boundaries and triggers.
     */
    @Getter
    private final WindowSpec spec;

    /**
     *
     * Constructs a new window function with the given window specification.
     * @param spec the window specification
     */
    public WindowFunction(WindowSpec spec) {
        this.spec = spec;
    }
}
