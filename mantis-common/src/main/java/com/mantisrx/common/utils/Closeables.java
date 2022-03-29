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

package com.mantisrx.common.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.IOExceptionList;

public class Closeables {
    /**
     * Combines a list of closeables into a composite closeable which when called closes the closeables one-by-one.
     *
     * @param closeables closeables that all need to be closed
     * @return a composite closeable
     */
    public static Closeable combine(Collection<? extends Closeable> closeables) {
        return combine(closeables.toArray(new Closeable[0]));
    }

    public static Closeable combine(Closeable... closeables) {
        return () -> {
            List<Throwable> list = new ArrayList<>();
            for (Closeable closeable: closeables) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    list.add(new Exception(String.format("Failed to close %s", closeable), e));
                }
            }

            if (!list.isEmpty()) {
                throw new IOExceptionList(list);
            }
        };
    }
}
