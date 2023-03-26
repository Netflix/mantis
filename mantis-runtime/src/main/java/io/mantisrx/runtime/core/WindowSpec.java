/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.runtime.core;

import java.time.Duration;
import lombok.Getter;

@Getter
public
class WindowSpec {
    private final WindowType type;
    private int numElements;
    private int elementOffset;
    private Duration windowLength;
    private Duration windowOffset;

    WindowSpec(WindowType type, Duration windowLength, Duration windowOffset) {
        this.type = type;
        this.windowLength = windowLength;
        this.windowOffset = windowOffset;
    }

    WindowSpec(WindowType type, int numElements, int elementOffset) {
        this.type = type;
        this.numElements = numElements;
        this.elementOffset = elementOffset;
    }

    /**
     *
     * Creates a time-based window specification for windows of the specified
     * length. The window type is {@link WindowType#TUMBLING}, which means that
     * non-overlapping windows are created.
     *
     * @param windowLength the length of the windows
     * @return the time-based window specification
     */
    public static WindowSpec timed(Duration windowLength) {
        return new WindowSpec(WindowType.TUMBLING, windowLength, windowLength);
    }

    /**
     *
     * Creates a time-based window specification for sliding windows of the specified
     * length and offset. The window type is {@link WindowType#SLIDING}, which means
     * that overlapping windows are created.
     *
     * @param windowLength the length of the windows
     * @param windowOffset the offset between the start of adjacent windows
     * @return the time-based window specification
     */
    public static WindowSpec timed(Duration windowLength, Duration windowOffset) {
        return new WindowSpec(WindowType.SLIDING, windowLength, windowOffset);
    }

    /**
     *
     * Creates an element-based window specification for windows of the specified
     * number of elements. The window type is {@link WindowType#ELEMENT}, which
     * means that non-overlapping windows are created.
     *
     * @param numElements the number of elements per window
     * @return the element-based window specification
     */
    public static WindowSpec count(int numElements) {
        return new WindowSpec(WindowType.ELEMENT, numElements, numElements);
    }

    /**
     *
     * Creates an element-based window specification for sliding windows of the
     * specified number of elements and offset. The window type is
     * {@link WindowType#ELEMENT_SLIDING}, which means that overlapping windows
     * are created.
     *
     * @param numElements the number of elements per window
     * @param elementOffset the offset between the start of adjacent windows
     * @return the element-based window specification
     */
    public static WindowSpec count(int numElements, int elementOffset) {
        return new WindowSpec(WindowType.ELEMENT_SLIDING, numElements, elementOffset);
    }

    public enum WindowType {
        TUMBLING,
        SLIDING,
        ELEMENT,
        ELEMENT_SLIDING
    }
}
