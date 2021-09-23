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

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import io.mantisrx.publish.internal.metrics.SpectatorUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.FullHttpRequest;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;


class GzipEncoder extends MessageToMessageEncoder<FullHttpRequest> {

    private final Registry registry;
    private final Timer encodeTime;

    GzipEncoder(Registry registry) {
        this.registry = registry;
        this.encodeTime =
                SpectatorUtils.buildAndRegisterTimer(
                        registry, "encodeTime",
                        "channel", HttpEventChannel.CHANNEL_TYPE,
                        "encoder", "gzip");
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) {
        final long start = registry.clock().wallTime();
        ByteBuf buf = ctx.alloc().directBuffer();
        try (
                ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
                GZIPOutputStream gos = new GZIPOutputStream(bbos)
        ) {
            msg.content().readBytes(gos, msg.content().readableBytes());
            gos.finish();
            FullHttpRequest message = msg.replace(buf.retain());

            out.add(message);
        } catch (Exception e) {
            ctx.fireExceptionCaught(new IOException("error encoding message", e));
        } finally {
            buf.release();
        }
        final long end = registry.clock().wallTime();
        encodeTime.record(end - start, TimeUnit.MILLISECONDS);
    }
}
