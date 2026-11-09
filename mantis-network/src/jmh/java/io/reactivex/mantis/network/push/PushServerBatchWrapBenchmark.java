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

package io.reactivex.mantis.network.push;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures assembling one batch for the wire: {@link PushServer#wrapBare}/{@link
 * PushServer#wrapDelimited} against the two-pass concatenation they replace.
 *
 * <p>{@code legacyBareVerbatim} and {@code legacyDelimitedVerbatim} are the <b>verbatim</b>
 * pre-change bodies, copied out of git rather than reimplemented so the comparison cannot drift:
 *
 * <pre>
 *   git show master:mantis-network/src/main/java/io/reactivex/mantis/network/push/PushServer.java
 * </pre>
 *
 * <p>Wrapping does not make bytes disappear, it moves who touches them, so measuring construction
 * alone would flatter it. Each path therefore has a {@code ...Gathered} arm that also walks the
 * finished buffer's {@link ByteBuf#nioBuffers() nioBuffers}, which is what the channel does on the
 * way to a gathering write. The honest claim is the pair of numbers, not the construction one.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class PushServerBatchWrapBenchmark {

    /**
     * Events in one 200ms window. 16 is Netty's default {@code maxNumComponents}, the silent
     * consolidation threshold this change had to opt out of; 1024 is {@code
     * MAX_WRAPPED_COMPONENTS}, above which the code deliberately falls back to concatenating; 2048
     * exercises that fallback. The fleet average is tens of events.
     */
    @Param({"30", "200"})
    public int events;

    /** Serialized event size. Mantis events run from a few hundred bytes to several KB. */
    @Param({"128", "256", "512", "1024", "2048", "4096", "16384"})
    public int eventBytes;

    private List<List<byte[]>> bufferOfBuffers;
    private byte[] prefix;
    private byte[] suffix;

    @Setup(Level.Trial)
    public void setup() {
        prefix = "data: ".getBytes();
        suffix = new byte[]{'\n', '\n'};

        // Shaped like the real thing: the batch arrives as a list of per-drain buffers, not one flat
        // list. Several small inner lists rather than one big one, which is what the chunkers emit.
        bufferOfBuffers = new ArrayList<>();
        List<byte[]> inner = new ArrayList<>();
        for (int i = 0; i < events; i++) {
            byte[] event = new byte[eventBytes];
            event[0] = (byte) i;
            event[eventBytes - 1] = (byte) i;
            inner.add(event);
            if (inner.size() == 8) {
                bufferOfBuffers.add(inner);
                inner = new ArrayList<>();
            }
        }
        if (!inner.isEmpty()) {
            bufferOfBuffers.add(inner);
        }

        // The whole change is only legitimate if the bytes on the wire are unchanged, so assert that
        // before measuring rather than trusting it.
        assertSameBytes(PushServer.wrapBare(bufferOfBuffers, events), legacyBare());
        assertSameBytes(PushServer.wrapDelimited(bufferOfBuffers, events, prefix, suffix),
                legacyDelimited());
    }

    private static void assertSameBytes(ByteBuf actual, byte[] expected) {
        try {
            byte[] got = new byte[actual.readableBytes()];
            actual.getBytes(actual.readerIndex(), got);
            if (got.length != expected.length) {
                throw new IllegalStateException(
                        "length differs: " + got.length + " vs " + expected.length);
            }
            for (int i = 0; i < got.length; i++) {
                if (got[i] != expected[i]) {
                    throw new IllegalStateException("bytes differ at " + i);
                }
            }
        } finally {
            actual.release();
        }
    }

    /**
     * The pre-change non-SSE path, verbatim. Do not "clean this up" -- its only value is being
     * byte-for-byte what shipped.
     */
    private byte[] legacyBare() {
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

    /** The pre-change uncompressed SSE path, verbatim. Same warning as above. */
    private byte[] legacyDelimited() {
        int totalBytes = 0;
        for (List<byte[]> buffer : bufferOfBuffers) {

            for (byte[] data : buffer) {
                totalBytes += (data.length + prefix.length + suffix.length);
            }
        }
        byte[] block = new byte[totalBytes];
        ByteBuffer blockBuffer = ByteBuffer.wrap(block);
        for (List<byte[]> buffer : bufferOfBuffers) {
            for (byte[] data : buffer) {
                blockBuffer.put(prefix);
                blockBuffer.put(data);
                blockBuffer.put(suffix);
            }
        }
        return blockBuffer.array();
    }

    /** Stands in for the channel resolving the buffer into an iovec array. */
    private static int gather(ByteBuf buf) {
        try {
            int total = 0;
            for (ByteBuffer nio : buf.nioBuffers()) {
                total += nio.remaining();
            }
            return total;
        } finally {
            buf.release();
        }
    }

    @Benchmark
    public ByteBuf wrapBare() {
        ByteBuf buf = PushServer.wrapBare(bufferOfBuffers, events);
        buf.release();
        return buf;
    }

    @Benchmark
    public int wrapBareGathered() {
        return gather(PushServer.wrapBare(bufferOfBuffers, events));
    }

    @Benchmark
    public byte[] legacyBareVerbatim() {
        return legacyBare();
    }

    @Benchmark
    public ByteBuf wrapDelimited() {
        ByteBuf buf = PushServer.wrapDelimited(bufferOfBuffers, events, prefix, suffix);
        buf.release();
        return buf;
    }

    @Benchmark
    public int wrapDelimitedGathered() {
        return gather(PushServer.wrapDelimited(bufferOfBuffers, events, prefix, suffix));
    }

    @Benchmark
    public byte[] legacyDelimitedVerbatim() {
        return legacyDelimited();
    }
}
