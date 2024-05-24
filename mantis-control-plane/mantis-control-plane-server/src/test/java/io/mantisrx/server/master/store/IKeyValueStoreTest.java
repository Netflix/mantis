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

package io.mantisrx.server.master.store;

import static junit.framework.TestCase.assertEquals;

import io.mantisrx.server.core.IKeyValueStore;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.time.Duration;
import org.junit.Test;

public class IKeyValueStoreTest {

    public static final String TABLE = "table";
    public static final String PK_1 = "pk1";
    public static final String V5 = "value5";
    public static final String V2 = "value2";
    public static final String V1 = "value1";
    public static final String V4 = "value4";

    @Test
    public void testUpsertOrdered() throws Exception {
        IKeyValueStore store = new io.mantisrx.server.master.store.InMemoryStore();
        store.upsertOrdered(TABLE, PK_1, 1L, V1, Duration.ZERO);
        store.upsertOrdered(TABLE, PK_1, 2L, V2, Duration.ZERO);
        store.upsertOrdered(TABLE, PK_1, 5L, V5, Duration.ZERO);
        store.upsertOrdered(TABLE, PK_1, 4L, V4, Duration.ZERO);
        assertEquals(ImmutableMap.<Long, String>of(5L, V5, 4L, V4, 2L, V2, 1L, V1), store.getAllOrdered(TABLE, PK_1, 5));
    }
}
