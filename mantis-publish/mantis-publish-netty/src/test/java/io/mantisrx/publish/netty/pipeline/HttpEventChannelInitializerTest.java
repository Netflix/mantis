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

import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import io.mantisrx.publish.config.MrePublishConfiguration;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class HttpEventChannelInitializerTest {

    private Channel channel;
    private HttpEventChannelInitializer initializer;

    @BeforeEach
    void setup() {
        Registry registry = new DefaultRegistry();
        MrePublishConfiguration config = mock(MrePublishConfiguration.class);
        channel = new EmbeddedChannel();
        initializer = new HttpEventChannelInitializer(registry, config, mock(EventLoopGroup.class));
    }

    @Test
    void shouldInitializeChannelPipelineWithExpectedHandlers() {
        List<String> expected = Arrays.asList(
                "LoggingHandler#0",
                "http-client-codec",
                "gzip-encoder",
                "http-object-aggregator",
                "write-timeout-handler",
                "mantis-event-aggregator",
                "idle-channel-handler",
                "event-channel-handler",
                "DefaultChannelPipeline$TailContext#0");

        initializer.initChannel(channel);
        Assertions.assertTrue(expected.containsAll(channel.pipeline().names()));
    }
}