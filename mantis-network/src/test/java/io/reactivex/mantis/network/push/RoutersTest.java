/*
 * Copyright 2025 Netflix, Inc.
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

package io.reactivex.mantis.network.push;

import org.junit.jupiter.api.Test;
import rx.functions.Func1;

import static io.reactivex.mantis.network.push.Routers.createRouterInstance;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RoutersTest {

    @Test
    void testCreateRouterInstanceSuccessfully() {
        final Func1<String, byte[]> toBytes = s -> s.getBytes();

        Router<String> router = Routers.createRouterInstance(
            RoundRobinRouter.class.getName(),
            "testRouter",
            toBytes
        );

        assertTrue(router instanceof RoundRobinRouter, "Expected instance of ConsistentHashingRouter");
    }

    @Test
    void testCreateRouterInstanceClassNotFound() {
        final Func1<String, byte[]> toBytes = s -> s.getBytes();
        System.out.println("HEEEEE");

        Router<String> router = createRouterInstance(
            "NonExistentRouterClass",
            "testRouter",
            toBytes
        );

        assertTrue(router instanceof RoundRobinRouter, "Expected instance of ConsistentHashingRouter");
    }
}
