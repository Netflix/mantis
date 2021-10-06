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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HeartbeatHandler extends ChannelDuplexHandler {

    static final Logger logger = LoggerFactory.getLogger(HeartbeatHandler.class);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
            throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                logger.warn("Read idle, due to missed heartbeats, closing connection: " + ctx.channel().remoteAddress());
                ctx.close();
            } else if (e.state() == IdleState.WRITER_IDLE) {
                ByteBuffer buffer = ByteBuffer.allocate(4 + 1); // length plus one byte
                buffer.putInt(1); // length of op code
                buffer.put((byte) 6); // op code for heartbeat for legacy protocol
                ctx.channel().writeAndFlush(buffer.array());
            }
        }
        super.userEventTriggered(ctx, evt);
    }
}
