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

import io.mantisrx.publish.netty.proto.MantisEvent;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.MessageToMessageEncoder;


/**
 * This class implements Netty's {@link MessageSizeEstimator} interface so it can be used by
 * a {@link MessageToMessageEncoder} to estimate the size of an object. This makes {@link MantisEvent}s compatible
 * with checking against Netty's {@link WriteBufferWaterMark}. Netty otherwise doesn't know how to estimate the
 * size of objects unfamiliar objects.
 */
final class MantisMessageSizeEstimator implements MessageSizeEstimator {

    /**
     * Return the default implementation which returns {@code 8} for unknown messages.
     */
    static final MessageSizeEstimator DEFAULT = new MantisMessageSizeEstimator();
    private static final DefaultMessageSizeEstimator.Handle NETTY_DEFAULT_HANDLE
            = new DefaultMessageSizeEstimator(8).newHandle();
    private final Handle handle;

    /**
     * Create a new instance.
     */
    private MantisMessageSizeEstimator() {
        handle = new HandleImpl();
    }

    @Override
    public Handle newHandle() {
        return handle;
    }

    private static final class HandleImpl implements Handle {

        @Override
        public int size(Object msg) {
            if (msg instanceof MantisEvent) {
                return ((MantisEvent) msg).size();
            }

            return NETTY_DEFAULT_HANDLE.size(msg);
        }
    }
}
