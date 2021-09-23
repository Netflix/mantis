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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spectator.api.DefaultRegistry;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class GzipEncoderTest {

    private ChannelHandlerContext ctx;
    private GzipEncoder encoder;
    private List<Object> out;

    @BeforeEach
    void setup() {
        ctx = mock(ChannelHandlerContext.class);
        when(ctx.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
        out = new ArrayList<>();
        encoder = new GzipEncoder(new DefaultRegistry());
    }

    @AfterEach
    void teardown() {
        out.clear();
        ctx.alloc().buffer().release();
    }

    @Test
    void shouldCompressFullHttpRequest() throws IOException {
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                "http://127.0.0.1:9090");
        request.content().writeBytes(new byte[1024]);
        int uncompressedSize = request.content().readableBytes();

        encoder.encode(ctx, request, out);
        assertFalse(out.isEmpty());

        FullHttpRequest result = (FullHttpRequest) out.get(0);
        int compressedSize = result.content().readableBytes();
        assertTrue(uncompressedSize > compressedSize);

        byte[] b = new byte[result.content().readableBytes()];
        result.content().readBytes(b);
        // Check first byte is from valid magic byte from GZIP header.
        assertEquals((byte) 0x8b1f, b[0]);
    }
}