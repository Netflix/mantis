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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.channel.ConnectionHandler;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfiguratorComposite;
import mantis.io.reactivex.netty.server.RxServer;
import rx.Observable;
import rx.functions.Func1;


public class LegacyTcpPushServer<T> extends PushServer<T, RemoteRxEvent> {

    private Func1<Map<String, List<String>>, Func1<T, Boolean>> predicate;
    private String name;
    private MeterRegistry meterRegistry;

    public LegacyTcpPushServer(PushTrigger<T> trigger, ServerConfig<T> config,
                               Observable<String> serverSignals) {
        super(trigger, config, serverSignals);
        this.predicate = config.getPredicate();
        this.name = config.getName();
        this.meterRegistry = config.getMeterRegistry();
    }

    @Override
    public RxServer<?, ?> createServer() {
        RxServer<RemoteRxEvent, RemoteRxEvent> server
                = RxNetty.newTcpServerBuilder(port, new ConnectionHandler<RemoteRxEvent, RemoteRxEvent>() {
            @Override
            public Observable<Void> handle(
                    final ObservableConnection<RemoteRxEvent, RemoteRxEvent> newConnection) {

                final InetSocketAddress socketAddress = (InetSocketAddress) newConnection.getChannel().remoteAddress();

                // extract groupId, id, predicate from incoming byte[]
                return
                        newConnection.getInput()
                                .flatMap(new Func1<RemoteRxEvent, Observable<Void>>() {

                                    @Override
                                    public Observable<Void> call(
                                            RemoteRxEvent incomingRequest) {

                                        if (incomingRequest.getType() == RemoteRxEvent.Type.subscribed) {
                                            Map<String, String> params = incomingRequest.getSubscribeParameters();

                                            // client state
                                            String id = null;
                                            String slotId = null;
                                            String groupId = null;
                                            // sample state
                                            boolean enableSampling = false;
                                            long samplingTimeMsec = 0;

                                            // predicate state
                                            Map<String, List<String>> predicateParams = null;

                                            if (params != null && !params.isEmpty()) {
                                                predicateParams = new HashMap<String, List<String>>();
                                                for (Entry<String, String> entry : params.entrySet()) {
                                                    List<String> values = new LinkedList<>();
                                                    values.add(entry.getValue());
                                                    predicateParams.put(entry.getKey(), values);
                                                }

                                                if (params.containsKey("id")) {
                                                    id = params.get("id");
                                                }
                                                if (params.containsKey("slotId")) {
                                                    slotId = params.get("slotId");
                                                }
                                                if (params.containsKey("groupId")) {
                                                    groupId = params.get("groupId");
                                                }
                                                if (params.containsKey("sample")) {
                                                    samplingTimeMsec = Long.parseLong(params.get("sample")) * 1000;
                                                    if (samplingTimeMsec < 50) {
                                                        throw new IllegalArgumentException("Sampling rate too low: " + samplingTimeMsec);
                                                    }
                                                    enableSampling = true;
                                                }
                                                if (params.containsKey("sampleMSec")) {
                                                    samplingTimeMsec = Long.parseLong(params.get("sampleMSec"));
                                                    if (samplingTimeMsec < 50) {
                                                        throw new IllegalArgumentException("Sampling rate too low: " + samplingTimeMsec);
                                                    }
                                                    enableSampling = true;
                                                }
                                            }
                                            Func1<T, Boolean> predicateFunction = null;
                                            if (predicate != null) {
                                                predicateFunction = predicate.call(predicateParams);
                                            }

                                            // support legacy metrics per connection

                                            Counter legacyMsgProcessedCounter = meterRegistry.counter("DropOperator_outgoing_subject_" + slotId + "onNext");
                                            Counter legacyDroppedWrites = meterRegistry.counter("DropOperator_outgoing_subject_" + slotId + "dropped");


                                            return manageConnection(newConnection, socketAddress.getHostString(), socketAddress.getPort(),
                                                    groupId, slotId, id, null,
                                                    false, null, enableSampling, samplingTimeMsec,
                                                    predicateFunction, null, legacyMsgProcessedCounter, legacyDroppedWrites,
                                                    null);
                                        }
                                        return null;
                                    }
                                });
            }
        })
                .pipelineConfigurator(new PipelineConfiguratorComposite<RemoteRxEvent, RemoteRxEvent>(
                        new PipelineConfigurator<RemoteRxEvent, RemoteRxEvent>() {
                            @Override
                            public void configureNewPipeline(ChannelPipeline pipeline) {

                                //					pipeline.addLast(new LoggingHandler(LogLevel.ERROR)); // uncomment to enable debug logging
                                pipeline.addLast("idleStateHandler", new IdleStateHandler(10, 2, 0));
                                pipeline.addLast("heartbeat", new HeartbeatHandler());
                                pipeline.addLast("gzipInflater", new JdkZlibEncoder(ZlibWrapper.GZIP));
                                pipeline.addLast("gzipDeflater", new JdkZlibDecoder(ZlibWrapper.GZIP));
                                pipeline.addLast("frameEncoder", new LengthFieldPrepender(4)); // 4 bytes to encode length
                                pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(5242880, 0, 4, 0, 4)); // max frame = half MB

                            }
                        }, new LegacyTcpPipelineConfigurator(name)))
                .channelOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(1024 * 1024, 5 * 1024 * 1024))

                .build();
        return server;
    }
}
