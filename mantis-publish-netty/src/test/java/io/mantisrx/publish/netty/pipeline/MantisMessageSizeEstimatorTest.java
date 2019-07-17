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

package io.mantisrx.publish.netty.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.mantisrx.publish.netty.proto.MantisEvent;
import io.mantisrx.publish.proto.MantisServerSubscription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.MessageSizeEstimator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


class MantisMessageSizeEstimatorTest {

    private MessageSizeEstimator estimator;
    private MessageSizeEstimator.Handle handle;

    @BeforeEach
    void setup() {
        estimator = MantisMessageSizeEstimator.DEFAULT;
        handle = estimator.newHandle();
    }

    @Test
    void shouldReturnSizeForMantisMessage() {
        MantisEvent event = new MantisEvent(1, "v");
        assertEquals(5, handle.size(event));
    }

    @Test
    void shouldReturnSizeForNettyByteBuf() {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeByte(1);
        assertEquals(1, buf.readableBytes());
        buf.release();
    }

    @Test
    void shouldReturnUnknownSizeForUnsupportedObject() {
        MantisServerSubscription subscription = Mockito.mock(MantisServerSubscription.class);
        // Delegate to Netty's default handle which returns 8 for unknown size
        // since our handle (nor Netty's) knows how to estimate a MantisServerSubscription object.
        assertEquals(8, handle.size(subscription));
    }
}