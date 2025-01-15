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

package io.reactivex.mantis.remote.observable;

import com.mantisrx.common.utils.NettyUtils;
import io.mantisrx.common.MantisGroup;
import io.mantisrx.common.codec.Decoder;
import io.mantisrx.common.codec.Encoder;
import io.mantisrx.server.core.ServiceRegistry;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.reactivex.mantis.remote.observable.ingress.IngressPolicies;
import io.reactivex.mantis.remote.observable.ingress.IngressPolicy;
import io.reactivx.mantis.operators.DropOperator;
import io.reactivx.mantis.operators.GroupedObservableUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfiguratorComposite;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Notification;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observables.GroupedObservable;


public class RemoteObservable {

    private static final Logger logger = LoggerFactory.getLogger(RemoteObservable.class);

    private static boolean enableHeartBeating = true;
    private static boolean enableNettyLogging = false;
    private static boolean enableCompression = true;
    private static int maxFrameLength = 5242880; // 5 MB max frame
    private static int bufferSize = 0;
    private static final String DEFAULT_BUFFER_SIZE_STR = "0";



    // NJ
    static {
        NettyUtils.setNettyThreads();

    }


    private RemoteObservable() { }

    private static void loadFastProperties() {
        String enableHeartBeatingStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.enableHeartBeating", "true");
        if (enableHeartBeatingStr.equals("false")) {
            enableHeartBeating = false;
        }

        String enableNettyLoggingStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.enableLogging", "false");
        if (enableNettyLoggingStr.equals("true")) {
            enableNettyLogging = true;
        }

        String enableCompressionStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.enableCompression", "true");
        if (enableCompressionStr.equals("false")) {
            enableCompression = false;
        }

        String maxFrameLengthStr =
                ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("mantis.netty.maxFrameLength", "5242880");
        if (maxFrameLengthStr != null && maxFrameLengthStr.length() > 0) {
            maxFrameLength = Integer.parseInt(maxFrameLengthStr);
        }

        String bufferSizeStr = ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("workerClient.buffer.size", DEFAULT_BUFFER_SIZE_STR);
        bufferSize = Integer.parseInt(Optional.ofNullable(bufferSizeStr).orElse(DEFAULT_BUFFER_SIZE_STR));
    }

    private static Func1<? super Observable<? extends Throwable>, ? extends Observable<?>> retryLogic(final
                                                                                                      ConnectToConfig params) {
        return new Func1<Observable<? extends Throwable>, Observable<?>>() {
            @Override
            public Observable<?> call(
                    Observable<? extends Throwable> attempts) {
                return
                        attempts
                                .zipWith(Observable.range(1, params.getSubscribeAttempts())
                                        , new Func2<Throwable, Integer, ThrowableWithCount>() {
                                            @Override
                                            public ThrowableWithCount call(Throwable t1, Integer retryAttempt) {

                                                return new ThrowableWithCount(t1, retryAttempt);
                                            }
                                        })
                                .flatMap(new Func1<ThrowableWithCount, Observable<?>>() {
                                    @Override
                                    public Observable<?> call(ThrowableWithCount notificationWithCount) {
                                        logger.debug("Failed to subscribe to remote observable: " + params.getName(),
                                                notificationWithCount.getThrowable());
                                        logger.info("Failed to subscribe to remote observable: " + params.getName() + ""
                                                + " at host: " + params.getHost() + " on port: " + params.getPort() + " subscribe "
                                                + "attempt: " + notificationWithCount.getCount() + " of: " + params.getSubscribeAttempts());
                                        if (notificationWithCount.getCount() == params.getSubscribeAttempts()) {
                                            Throwable t = notificationWithCount.getThrowable();
                                            if (t != null) {
                                                return Observable.error(notificationWithCount.getThrowable());
                                            }
                                        }
                                        return Observable.timer(notificationWithCount.getCount(), TimeUnit.SECONDS);
                                    }
                                });
            }
        };
    }

    public static <T> RemoteRxConnection<T> connect(final ConnectToObservable<T> params) {
        final RxMetrics metrics = new RxMetrics();

        return new RemoteRxConnection<T>(Observable.create(new OnSubscribe<T>() {
            @Override
            public void call(Subscriber<? super T> subscriber) {
                RemoteUnsubscribe remoteUnsubscribe = new RemoteUnsubscribe(params.getName());
                // wrapped in Observable.create() to inject unsubscribe callback
                subscriber.add(remoteUnsubscribe); // unsubscribed callback
                // create connection
                createTcpConnectionToServer(params, remoteUnsubscribe, metrics,
                        params.getConnectionDisconnectCallback(),
                        params.getCloseTrigger())
                        .subscribe(subscriber);
            }
        }), metrics, params.getCloseTrigger());
    }

    public static <K, V> RemoteRxConnection<GroupedObservable<K, V>> connect(
            final ConnectToGroupedObservable<K, V> config) {
        final RxMetrics metrics = new RxMetrics();
        return new RemoteRxConnection<GroupedObservable<K, V>>(Observable.create(
                new OnSubscribe<GroupedObservable<K, V>>() {
                    @Override
                    public void call(Subscriber<? super GroupedObservable<K, V>> subscriber) {
                        RemoteUnsubscribe remoteUnsubscribe = new RemoteUnsubscribe(config.getName());
                        // wrapped in Observable.create() to inject unsubscribe callback
                        subscriber.add(remoteUnsubscribe); // unsubscribed callback
                        // create connection
                        createTcpConnectionToServer(config, remoteUnsubscribe, metrics,
                                config.getConnectionDisconnectCallback(),
                                config.getCloseTrigger())
                                .retryWhen(retryLogic(config))
                                .subscribe(subscriber);
                    }
                }), metrics, config.getCloseTrigger());
    }

    // NJ
    public static <K, V> RemoteRxConnection<MantisGroup<K, V>> connectToMGO(
            final ConnectToGroupedObservable<K, V> config, final SpscArrayQueue<MantisGroup<?, ?>> inputQueue) {
        final RxMetrics metrics = new RxMetrics();
        return new RemoteRxConnection<MantisGroup<K, V>>(Observable.create(
                new OnSubscribe<MantisGroup<K, V>>() {
                    @Override
                    public void call(Subscriber<? super MantisGroup<K, V>> subscriber) {
                        RemoteUnsubscribe remoteUnsubscribe = new RemoteUnsubscribe(config.getName());
                        // wrapped in Observable.create() to inject unsubscribe callback
                        subscriber.add(remoteUnsubscribe); // unsubscribed callback
                        // create connection
                        createTcpConnectionToGOServer(config, remoteUnsubscribe, metrics,
                                config.getConnectionDisconnectCallback(),
                                config.getCloseTrigger(),
                                inputQueue)
                                .retryWhen(retryLogic(config))

                                .subscribe(subscriber);
                    }
                }), metrics, config.getCloseTrigger());
    }


    public static <T> Observable<T> connect(final String host, final int port, final Decoder<T> decoder) {
        return connect(new ConnectToObservable.Builder<T>()
                .host(host)
                .port(port)
                .decoder(decoder)
                .build()).getObservable();
    }

    private static <K, V> Observable<GroupedObservable<K, V>> createTcpConnectionToServer(final ConnectToGroupedObservable<K, V> params,
                                                                                          final RemoteUnsubscribe remoteUnsubscribe, final RxMetrics metrics,
                                                                                          final Action0 connectionDisconnectCallback, Observable<Integer> closeTrigger) {
        final Decoder<K> keyDecoder = params.getKeyDecoder();
        final Decoder<V> valueDecoder = params.getValueDecoder();
        loadFastProperties();
        return
                RxNetty.createTcpClient(params.getHost(), params.getPort(), new PipelineConfiguratorComposite<RemoteRxEvent, List<RemoteRxEvent>>(
                        new PipelineConfigurator<RemoteRxEvent, List<RemoteRxEvent>>() {
                            @Override
                            public void configureNewPipeline(ChannelPipeline pipeline) {
                                if (enableNettyLogging) {
                                    pipeline.addFirst(new LoggingHandler(LogLevel.ERROR)); // uncomment to enable debug logging
                                }
                                if (enableHeartBeating) {
                                    pipeline.addLast("idleStateHandler", new IdleStateHandler(10, 2, 0));
                                    pipeline.addLast("heartbeat", new HeartbeatHandler());
                                }
                                if (enableCompression) {
                                    pipeline.addLast("gzipInflater", new JdkZlibEncoder(ZlibWrapper.GZIP));
                                    pipeline.addLast("gzipDeflater", new JdkZlibDecoder(ZlibWrapper.GZIP));
                                }
                                pipeline.addLast("frameEncoder", new LengthFieldPrepender(4)); // 4 bytes to encode length
                                pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4)); // max frame = half MB
                            }
                        }, new BatchedRxEventPipelineConfigurator()))
                        .connect()
                        // send subscription request, get input stream
                        .flatMap(new Func1<ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>>, Observable<RemoteRxEvent>>() {
                            @Override
                            public Observable<RemoteRxEvent> call(final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection) {
                                connection.writeAndFlush(RemoteRxEvent.subscribed(params.getName(), params.getSubscribeParameters())); // send subscribe event to server
                                remoteUnsubscribe.setConnection(connection);
                                return connection.getInput()
                                        .lift(new DropOperator<RemoteRxEvent>("incoming_" + RemoteObservable.class.getCanonicalName() + "_createTcpConnectionToServerGroups"))
                                        .rebatchRequests(bufferSize <= 0 ? 1 : bufferSize);
                            }
                        })
                        .doOnCompleted(new Action0() {
                            @Override
                            public void call() {
                                // connection completed
                                logger.warn("Detected connection completed when trying to connect to host: " + params.getHost() + " port: " + params.getPort());
                                connectionDisconnectCallback.call();
                            }
                        })
                        .onErrorResumeNext(new Func1<Throwable, Observable<RemoteRxEvent>>() {
                            @Override
                            public Observable<RemoteRxEvent> call(Throwable t1) {
                                logger.warn("Detected connection error when trying to connect to host: " + params.getHost() + " port: " + params.getPort(), t1);
                                connectionDisconnectCallback.call();
                                // complete if error occurs
                                return Observable.empty();
                            }
                        })
                        .takeUntil(closeTrigger)
                        // data received from server
                        .map(new Func1<RemoteRxEvent, Notification<byte[]>>() {
                            @Override
                            public Notification<byte[]> call(RemoteRxEvent rxEvent) {
                                if (rxEvent.getType() == RemoteRxEvent.Type.next) {
                                    metrics.incrementNextCount();
                                    return Notification.createOnNext(rxEvent.getData());
                                } else if (rxEvent.getType() == RemoteRxEvent.Type.error) {
                                    metrics.incrementErrorCount();
                                    return Notification.createOnError(fromBytesToThrowable(rxEvent.getData()));
                                } else if (rxEvent.getType() == RemoteRxEvent.Type.completed) {
                                    metrics.incrementCompletedCount();
                                    return Notification.createOnCompleted();
                                } else {
                                    throw new RuntimeException("RemoteRxEvent of type:" + rxEvent.getType() + ", not supported.");
                                }
                            }
                        })
                        .<byte[]>dematerialize()
                        .groupBy(
                                new Func1<byte[], K>() {
                                    @Override
                                    public K call(byte[] bytes) {
                                        ByteBuffer buff = ByteBuffer.wrap((byte[]) bytes);
                                        buff.get(); // ignore notification type in key selector
                                        int keyLength = buff.getInt();
                                        byte[] key = new byte[keyLength];
                                        buff.get(key);
                                        return keyDecoder.decode(key);
                                    }
                                }
                                , new Func1<byte[], Notification<V>>() {
                                    @Override
                                    public Notification<V> call(byte[] bytes) {
                                        ByteBuffer buff = ByteBuffer.wrap((byte[]) bytes);
                                        byte notificationType = buff.get();
                                        if (notificationType == 1) {
                                            int keyLength = buff.getInt();
                                            int end = buff.limit();
                                            int dataLength = end - 4 - 1 - keyLength;
                                            byte[] valueBytes = new byte[dataLength];
                                            buff.position(4 + 1 + keyLength);
                                            buff.get(valueBytes, 0, dataLength);
                                            V value = valueDecoder.decode(valueBytes);
                                            return Notification.createOnNext(value);
                                        } else if (notificationType == 2) {
                                            return Notification.createOnCompleted();
                                        } else if (notificationType == 3) {
                                            int keyLength = buff.getInt();
                                            int end = buff.limit();
                                            int dataLength = end - 4 - 1 - keyLength;
                                            byte[] errorBytes = new byte[dataLength];
                                            buff.position(4 + 1 + keyLength);
                                            buff.get(errorBytes, 0, dataLength);
                                            return Notification.createOnError(fromBytesToThrowable(errorBytes));
                                        } else {
                                            throw new RuntimeException("Notification encoding not support: " + notificationType);
                                        }
                                    }
                                })
                        .map(new Func1<GroupedObservable<K, Notification<V>>, GroupedObservable<K, V>>() {
                            @Override
                            public GroupedObservable<K, V> call(
                                    GroupedObservable<K, Notification<V>> group) {
                                return GroupedObservableUtils.createGroupedObservable(
                                        group.getKey(), group.<V>dematerialize());
                            }
                        })
                        .doOnEach(new Observer<GroupedObservable<K, V>>() {
                            @Override
                            public void onCompleted() {
                                logger.info("RemoteRxEvent, name: {} onCompleted()", params.getName());

                            }

                            @Override
                            public void onError(Throwable e) {
                                logger.error("RemoteRxEvent, name: {} onError()", params.getName(), e);
                            }

                            @Override
                            public void onNext(GroupedObservable<K, V> group) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("RemoteRxEvent, name: {} new key: {}", params.getName(), group.getKey());
                                }
                            }
                        });
    }

    private static <K, V> Observable<MantisGroup<K, V>> createTcpConnectionToGOServer(final ConnectToGroupedObservable<K, V> params,
                                                                                      final RemoteUnsubscribe remoteUnsubscribe, final RxMetrics metrics,
                                                                                      final Action0 connectionDisconnectCallback, Observable<Integer> closeTrigger,
                                                                                      final SpscArrayQueue<MantisGroup<?, ?>> inputQueue) {
        final Decoder<K> keyDecoder = params.getKeyDecoder();
        final Decoder<V> valueDecoder = params.getValueDecoder();
        loadFastProperties();
        return
                RxNetty.createTcpClient(params.getHost(), params.getPort(), new PipelineConfiguratorComposite<RemoteRxEvent, List<RemoteRxEvent>>(
                        new PipelineConfigurator<RemoteRxEvent, List<RemoteRxEvent>>() {
                            @Override
                            public void configureNewPipeline(ChannelPipeline pipeline) {
                                if (enableNettyLogging) {
                                    pipeline.addFirst(new LoggingHandler(LogLevel.ERROR)); // uncomment to enable debug logging
                                }
                                if (enableHeartBeating) {
                                    pipeline.addLast("idleStateHandler", new IdleStateHandler(10, 2, 0));
                                    pipeline.addLast("heartbeat", new HeartbeatHandler());
                                }
                                if (enableCompression) {
                                    pipeline.addLast("gzipInflater", new JdkZlibEncoder(ZlibWrapper.GZIP));
                                    pipeline.addLast("gzipDeflater", new JdkZlibDecoder(ZlibWrapper.GZIP));
                                }
                                pipeline.addLast("frameEncoder", new LengthFieldPrepender(4)); // 4 bytes to encode length
                                pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4)); // max frame = half MB
                            }
                        }, new BatchedRxEventPipelineConfigurator()))
                        .connect()
                        // send subscription request, get input stream
                        .flatMap(new Func1<ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>>, Observable<RemoteRxEvent>>() {
                            @Override
                            public Observable<RemoteRxEvent> call(final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection) {
                                connection.writeAndFlush(RemoteRxEvent.subscribed(params.getName(), params.getSubscribeParameters())); // send subscribe event to server
                                remoteUnsubscribe.setConnection(connection);
                                return connection.getInput()
                                        .lift(new DropOperator<RemoteRxEvent>("incoming_" + RemoteObservable.class.getCanonicalName() + "_createTcpConnectionToServerGroups"))
                                        .rebatchRequests(bufferSize <= 0 ? 1 : bufferSize);
                            }
                        })
                        .doOnCompleted(new Action0() {
                            @Override
                            public void call() {
                                // connection completed
                                logger.warn("Detected connection completed when trying to connect to host: " + params.getHost() + " port: " + params.getPort());
                                connectionDisconnectCallback.call();
                            }
                        })
                        .onErrorResumeNext(new Func1<Throwable, Observable<RemoteRxEvent>>() {
                            @Override
                            public Observable<RemoteRxEvent> call(Throwable t1) {
                                logger.warn("Detected connection error when trying to connect to host: " + params.getHost() + " port: " + params.getPort(), t1);
                                connectionDisconnectCallback.call();
                                // complete if error occurs
                                return Observable.empty();
                            }
                        })
                        .takeUntil(closeTrigger)
                        .filter(new Func1<RemoteRxEvent, Boolean>() {

                            @Override
                            public Boolean call(RemoteRxEvent rxEvent) {
                                return (rxEvent.getType() == RemoteRxEvent.Type.next);
                            }

                        })
                        // data received from server
                        .map(new Func1<RemoteRxEvent, Notification<byte[]>>() {
                            @Override
                            public Notification<byte[]> call(RemoteRxEvent rxEvent) {
                                metrics.incrementNextCount();
                                return Notification.createOnNext(rxEvent.getData());
                            }
                        })
                        .<byte[]>dematerialize()
                        .map(new Func1<byte[], MantisGroup<K, V>>() {

                            @Override
                            public MantisGroup<K, V> call(byte[] bytes) {
                                ByteBuffer buff = ByteBuffer.wrap((byte[]) bytes);
                                buff.get(); // ignore notification type in key selector
                                int keyLength = buff.getInt();
                                byte[] key = new byte[keyLength];
                                buff.get(key);
                                K keyVal = keyDecoder.decode(key);
                                V value = null;
                                buff = ByteBuffer.wrap((byte[]) bytes);
                                byte notificationType = buff.get();
                                if (notificationType == 1) {
                                    //int keyLength = buff.getInt();
                                    int end = buff.limit();
                                    int dataLength = end - 4 - 1 - keyLength;
                                    byte[] valueBytes = new byte[dataLength];
                                    buff.position(4 + 1 + keyLength);
                                    buff.get(valueBytes, 0, dataLength);
                                    value = valueDecoder.decode(valueBytes);

                                } else {
                                    throw new RuntimeException("Notification encoding not support: " + notificationType);
                                }
                                return new MantisGroup<K, V>(keyVal, value);
                            }

                        })
                        .doOnEach(new Observer<MantisGroup<K, V>>() {
                            @Override
                            public void onCompleted() {
                                logger.info("RemoteRxEvent, name: " + params.getName() + " onCompleted()");
                            }

                            @Override
                            public void onError(Throwable e) {
                                logger.error("RemoteRxEvent, name: " + params.getName() + " onError()", e);
                            }

                            @Override
                            public void onNext(MantisGroup<K, V> group) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("RemoteRxEvent, name: " + params.getName() + " new key: " + group.getKeyValue());
                                }
                            }
                        });

    }


    private static <T> Observable<T> createTcpConnectionToServer(final ConnectToObservable<T> params,
                                                                 final RemoteUnsubscribe remoteUnsubscribe, final RxMetrics metrics,
                                                                 final Action0 connectionDisconnectCallback, Observable<Integer> closeTrigger) {

        final Decoder<T> decoder = params.getDecoder();
        loadFastProperties();
        return
                RxNetty.createTcpClient(params.getHost(), params.getPort(), new PipelineConfiguratorComposite<RemoteRxEvent, List<RemoteRxEvent>>(
                        new PipelineConfigurator<RemoteRxEvent, List<RemoteRxEvent>>() {
                            @Override
                            public void configureNewPipeline(ChannelPipeline pipeline) {
                                if (enableNettyLogging) {
                                    pipeline.addFirst(new LoggingHandler(LogLevel.ERROR)); // uncomment to enable debug logging
                                }
                                if (enableHeartBeating) {
                                    pipeline.addLast("idleStateHandler", new IdleStateHandler(10, 2, 0));
                                    pipeline.addLast("heartbeat", new HeartbeatHandler());
                                }
                                if (enableCompression) {
                                    pipeline.addLast("gzipInflater", new JdkZlibEncoder(ZlibWrapper.GZIP));
                                    pipeline.addLast("gzipDeflater", new JdkZlibDecoder(ZlibWrapper.GZIP));
                                }
                                pipeline.addLast("frameEncoder", new LengthFieldPrepender(4)); // 4 bytes to encode length
                                pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4)); // max frame = half MB

                            }
                        }, new BatchedRxEventPipelineConfigurator()))
                        .connect()
                        // send subscription request, get input stream
                        .flatMap(new Func1<ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>>, Observable<RemoteRxEvent>>() {
                            @Override
                            public Observable<RemoteRxEvent> call(final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection) {
                                connection.writeAndFlush(RemoteRxEvent.subscribed(params.getName(), params.getSubscribeParameters())); // send subscribe event to server
                                remoteUnsubscribe.setConnection(connection);
                                return connection.getInput()
                                        .lift(new DropOperator<RemoteRxEvent>("incoming_" + RemoteObservable.class.getCanonicalName() + "_createTcpConnectionToServer"))
                                        .rebatchRequests(bufferSize <= 0 ? 1 : bufferSize);
                            }
                        })
                        .doOnCompleted(new Action0() {
                            @Override
                            public void call() {
                                // connection completed
                                logger.warn("Detected connection completed when trying to connect to host: " + params.getHost() + " port: " + params.getPort());
                                connectionDisconnectCallback.call();
                            }
                        })
                        .onErrorResumeNext(new Func1<Throwable, Observable<RemoteRxEvent>>() {
                            @Override
                            public Observable<RemoteRxEvent> call(Throwable t1) {
                                logger.warn("Detected connection error when trying to connect to host: " + params.getHost() + " port: " + params.getPort(), t1);
                                connectionDisconnectCallback.call();
                                // complete if error occurs
                                return Observable.empty();
                            }
                        })
                        .takeUntil(closeTrigger)
                        .map(new Func1<RemoteRxEvent, Notification<T>>() {
                            @Override
                            public Notification<T> call(RemoteRxEvent rxEvent) {
                                if (rxEvent.getType() == RemoteRxEvent.Type.next) {
                                    metrics.incrementNextCount();
                                    return Notification.createOnNext(decoder.decode(rxEvent.getData()));
                                } else if (rxEvent.getType() == RemoteRxEvent.Type.error) {
                                    metrics.incrementErrorCount();
                                    return Notification.createOnError(fromBytesToThrowable(rxEvent.getData()));
                                } else if (rxEvent.getType() == RemoteRxEvent.Type.completed) {
                                    metrics.incrementCompletedCount();
                                    return Notification.createOnCompleted();
                                } else {
                                    throw new RuntimeException("RemoteRxEvent of type: " + rxEvent.getType() + ", not supported.");
                                }
                            }
                        })
                        .<T>dematerialize()
                        .doOnEach(new Observer<T>() {
                            @Override
                            public void onCompleted() {
                                logger.info("RemoteRxEvent: " + params.getName() + " onCompleted()");
                            }

                            @Override
                            public void onError(Throwable e) {
                                logger.error("RemoteRxEvent: " + params.getName() + " onError()", e);
                            }

                            @Override
                            public void onNext(T t) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("RemoteRxEvent: " + params.getName() + " onNext(): " + t);
                                }
                            }
                        });
    }

    public static <T> RemoteRxServer serve(int port, final Observable<T> observable, final Encoder<T> encoder) {
        return new RemoteRxServer(configureServerFromParams(null, port, observable, encoder,
                IngressPolicies.allowAll()));
    }

    public static <T> RemoteRxServer serve(int port, String name, final Observable<T> observable, final Encoder<T> encoder) {
        return new RemoteRxServer(configureServerFromParams(name, port, observable, encoder,
                IngressPolicies.allowAll()));
    }

    private static <T> RemoteRxServer.Builder configureServerFromParams(String name, int port, Observable<T> observable,
                                                                        Encoder<T> encoder, IngressPolicy ingressPolicy) {
        return new RemoteRxServer
                .Builder()
                .port(port)
                .ingressPolicy(ingressPolicy)
                .addObservable(new ServeObservable.Builder<T>()
                        .name(name)
                        .encoder(encoder)
                        .observable(observable)
                        .subscriptionPerConnection()
                        .build());
    }

    static byte[] fromThrowableToBytes(Throwable t) {

        ByteArrayOutputStream baos = null;
        ObjectOutput out = null;
        try {
            // create a new exception
            // throw away data that may not
            // be serializable
            Constructor<? extends Throwable> con = t.getClass().getConstructor(String.class);
            Throwable newInstance = con.newInstance(t.getMessage());
            newInstance.setStackTrace(t.getStackTrace());
            baos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(baos);
            out.writeObject(newInstance);
        } catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            logger.error("Failed to convert throwable to bytes", e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (out != null) {out.close();}
                if (baos != null) {baos.close();}
            } catch (IOException e1) {
                e1.printStackTrace();
                throw new RuntimeException(e1);
            }
        }
        return baos.toByteArray();
    }

    static Throwable fromBytesToThrowable(byte[] bytes) {
        Throwable t = null;
        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            in = new ObjectInputStream(bis);
            t = (Throwable) in.readObject();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e1) {
            throw new RuntimeException(e1);
        } finally {
            try {
                if (bis != null) {bis.close();}
                if (in != null) {in.close();}
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return t;
    }
}
