/*
 * Copyright 2026 Netflix, Inc.
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

/**
 * Pins the bytes the legacy TCP outbound handler puts on the wire. The handler prepends a
 * two-or-more byte header to every write; the change under test stopped copying the payload behind
 * that header, so what matters is that the framing is unchanged.
 */
class LegacyTcpPipelineConfiguratorTest {

    private static final byte PROTOCOL_VERSION = 1;

    private static byte[] expectedFrame(String name, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(PROTOCOL_VERSION);
        if (name == null || name.isEmpty()) {
            out.write(0);
        } else {
            out.write(name.length());
            out.write(name.getBytes(), 0, name.getBytes().length);
        }
        out.write(payload, 0, payload.length);
        return out.toByteArray();
    }

    /** Reads everything the channel has queued outbound, concatenated. */
    private static byte[] drainOutbound(EmbeddedChannel channel) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuf buf;
        while ((buf = channel.readOutbound()) != null) {
            byte[] chunk = new byte[buf.readableBytes()];
            buf.readBytes(chunk);
            buf.release();
            out.write(chunk, 0, chunk.length);
        }
        return out.toByteArray();
    }

    private static EmbeddedChannel channelFor(String name) {
        EmbeddedChannel channel = new EmbeddedChannel();
        new LegacyTcpPipelineConfigurator(name).configureNewPipeline(channel.pipeline());
        return channel;
    }

    /**
     * The push server now hands the handler a composite buffer rather than a {@code byte[]}. Several
     * components must come out as one contiguous frame behind a single header.
     */
    @Test
    void compositeByteBufWriteIsFramedOnceAndNotPerComponent() {
        String name = "TestObservable";
        byte[] payload = "aaaabbbbcccc".getBytes();
        ByteBuf composite = Unpooled.wrappedBuffer(3,
                "aaaa".getBytes(), "bbbb".getBytes(), "cccc".getBytes());
        assertEquals(3, composite.nioBufferCount(), "test must write a genuine composite");

        EmbeddedChannel channel = channelFor(name);
        channel.writeOutbound(composite);
        assertArrayEquals(expectedFrame(name, payload), drainOutbound(channel));
        assertFalse(channel.finish());
    }

    @Test
    void singleByteBufWriteIsFramedIdentically() {
        String name = "TestObservable";
        byte[] payload = "0123456789".getBytes();

        EmbeddedChannel channel = channelFor(name);
        channel.writeOutbound(Unpooled.wrappedBuffer(payload));
        assertArrayEquals(expectedFrame(name, payload), drainOutbound(channel));
        assertFalse(channel.finish());
    }

    /** Heartbeats still arrive as {@code byte[]} and take the other branch. */
    @Test
    void byteArrayWriteIsFramedIdentically() {
        String name = "TestObservable";
        byte[] payload = "heartbeat".getBytes();

        EmbeddedChannel channel = channelFor(name);
        channel.writeOutbound(payload);
        assertArrayEquals(expectedFrame(name, payload), drainOutbound(channel));
        assertFalse(channel.finish());
    }

    /** With no observable name the header is version + a zero length byte. */
    @Test
    void emptyNameWritesZeroLengthHeader() {
        byte[] payload = "payload".getBytes();

        for (String name : new String[] {null, ""}) {
            EmbeddedChannel channel = channelFor(name);
            channel.writeOutbound(Unpooled.wrappedBuffer(payload));
            assertArrayEquals(expectedFrame(name, payload), drainOutbound(channel),
                    "name=" + name);
            assertFalse(channel.finish());
        }
    }
}
