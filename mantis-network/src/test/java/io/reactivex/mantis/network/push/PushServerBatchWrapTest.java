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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * The push write path used to concatenate every batch into a fresh {@code byte[totalBytes]} before
 * handing it to the channel; it now wraps the event arrays in a composite buffer instead. These
 * tests pin the new path to the old one byte for byte.
 *
 * <p>The baselines below ({@code legacyBare}, {@code legacyDelimited}) are the <b>verbatim
 * pre-change bodies</b> from {@code PushServer.startServer}, so the comparison cannot drift into
 * testing the new code against a restatement of itself.
 */
class PushServerBatchWrapTest {

    private static final byte[] SSE_PREFIX = "data: ".getBytes();
    private static final byte[] SSE_SUFFIX = "\n\n".getBytes();

    // ----------------------------------------------------------------------------------------
    // Verbatim pre-change concatenation. Do not tidy this up — its value is that it is what
    // production wrote before the change.
    // ----------------------------------------------------------------------------------------

    private static byte[] legacyBare(List<List<byte[]>> bufferOfBuffers) {
        int totalBytes = 0;
        for (List<byte[]> buffer : bufferOfBuffers) {
            for (byte[] data : buffer) {
                totalBytes += (data.length);
            }
        }
        byte[] block = new byte[totalBytes];
        ByteBuffer blockBuffer = ByteBuffer.wrap(block);
        for (List<byte[]> buffer : bufferOfBuffers) {
            for (byte[] data : buffer) {
                blockBuffer.put(data);
            }
        }
        return blockBuffer.array();
    }

    private static byte[] legacyDelimited(List<List<byte[]>> bufferOfBuffers, byte[] prefix,
                                          byte[] nwnw) {
        int totalBytes = 0;
        for (List<byte[]> buffer : bufferOfBuffers) {
            for (byte[] data : buffer) {
                totalBytes += (data.length + prefix.length + nwnw.length);
            }
        }
        byte[] block = new byte[totalBytes];
        ByteBuffer blockBuffer = ByteBuffer.wrap(block);
        for (List<byte[]> buffer : bufferOfBuffers) {
            for (byte[] data : buffer) {
                blockBuffer.put(prefix);
                blockBuffer.put(data);
                blockBuffer.put(nwnw);
            }
        }
        return blockBuffer.array();
    }

    // ----------------------------------------------------------------------------------------

    private static byte[] drain(ByteBuf buf) {
        byte[] out = new byte[buf.readableBytes()];
        buf.readBytes(out);
        buf.release();
        return out;
    }

    private static int countEvents(List<List<byte[]>> bufferOfBuffers) {
        int size = 0;
        for (List<byte[]> buffer : bufferOfBuffers) {
            size += buffer.size();
        }
        return size;
    }

    /** Batch shapes worth covering: empty inner lists, empty payloads, one event, many events. */
    private static List<List<List<byte[]>>> batchShapes() {
        Random rnd = new Random(20260817L);
        List<List<List<byte[]>>> shapes = new ArrayList<>();

        shapes.add(Collections.singletonList(Collections.singletonList("one".getBytes())));
        shapes.add(Collections.singletonList(Collections.<byte[]>emptyList()));
        shapes.add(Arrays.asList(Collections.<byte[]>emptyList(),
                Collections.singletonList("after empty".getBytes()),
                Collections.<byte[]>emptyList()));
        shapes.add(Collections.singletonList(Arrays.asList(new byte[0], "x".getBytes(), new byte[0])));

        for (int shape = 0; shape < 12; shape++) {
            List<List<byte[]>> batch = new ArrayList<>();
            int inner = 1 + rnd.nextInt(6);
            for (int i = 0; i < inner; i++) {
                int events = rnd.nextInt(8);
                List<byte[]> buffer = new ArrayList<>(events);
                for (int e = 0; e < events; e++) {
                    byte[] data = new byte[rnd.nextInt(512)];
                    rnd.nextBytes(data);
                    buffer.add(data);
                }
                batch.add(buffer);
            }
            shapes.add(batch);
        }
        return shapes;
    }

    @Test
    void bareWrapMatchesLegacyConcatenation() {
        for (List<List<byte[]>> batch : batchShapes()) {
            assertArrayEquals(legacyBare(batch),
                    drain(PushServer.wrapBare(batch, countEvents(batch))),
                    "bare batch of " + countEvents(batch) + " events");
        }
    }

    @Test
    void delimitedWrapMatchesLegacyConcatenation() {
        for (List<List<byte[]>> batch : batchShapes()) {
            assertArrayEquals(legacyDelimited(batch, SSE_PREFIX, SSE_SUFFIX),
                    drain(PushServer.wrapDelimited(batch, countEvents(batch), SSE_PREFIX, SSE_SUFFIX)),
                    "delimited batch of " + countEvents(batch) + " events");
        }
    }

    /** The compressed-SSE branch is a fixed three-part frame. */
    @Test
    void compressedFrameWrapMatchesLegacyConcatenation() {
        byte[] compressed = new byte[977];
        new Random(7L).nextBytes(compressed);

        ByteBuffer legacy = ByteBuffer.allocate(SSE_PREFIX.length + compressed.length + SSE_SUFFIX.length);
        legacy.put(SSE_PREFIX);
        legacy.put(compressed);
        legacy.put(SSE_SUFFIX);

        assertArrayEquals(legacy.array(), drain(PushServer.wrap(SSE_PREFIX, compressed, SSE_SUFFIX)));
    }

    /**
     * Past {@link PushServer#MAX_WRAPPED_COMPONENTS} the wrap falls back to concatenating. The
     * fallback is the thing most likely to rot unnoticed, since it never runs in the common case,
     * so it is asserted both to trigger and to produce the same bytes.
     */
    @Test
    void oversizedBatchFallsBackToCopyWithIdenticalBytes() {
        Random rnd = new Random(99L);
        List<byte[]> buffer = new ArrayList<>();
        for (int i = 0; i < PushServer.MAX_WRAPPED_COMPONENTS + 5; i++) {
            byte[] data = new byte[1 + rnd.nextInt(4)];
            rnd.nextBytes(data);
            buffer.add(data);
        }
        List<List<byte[]>> batch = Collections.singletonList(buffer);
        int events = countEvents(batch);
        assertTrue(events > PushServer.MAX_WRAPPED_COMPONENTS, "test must exercise the fallback");

        ByteBuf wrapped = PushServer.wrapBare(batch, events);
        assertEquals(1, wrapped.nioBufferCount(), "oversized batch should be a single buffer");
        assertArrayEquals(legacyBare(batch), drain(wrapped));

        assertArrayEquals(legacyDelimited(batch, SSE_PREFIX, SSE_SUFFIX),
                drain(PushServer.wrapDelimited(batch, events, SSE_PREFIX, SSE_SUFFIX)));
    }

    /**
     * The point of the change: a normal batch is written as N components rather than copied into
     * one array. Without an explicit component limit {@code Unpooled.wrappedBuffer} silently
     * consolidates above 16 components, which would make the change a no-op.
     */
    @Test
    void normalBatchIsNotCopiedIntoOneBuffer() {
        List<byte[]> buffer = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            buffer.add(("event-" + i).getBytes());
        }
        List<List<byte[]>> batch = Collections.singletonList(buffer);

        ByteBuf bare = PushServer.wrapBare(batch, 64);
        assertEquals(64, bare.nioBufferCount());
        bare.release();

        ByteBuf delimited = PushServer.wrapDelimited(batch, 64, SSE_PREFIX, SSE_SUFFIX);
        assertEquals(64 * 3, delimited.nioBufferCount());
        delimited.release();
    }
}
