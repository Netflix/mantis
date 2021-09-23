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

import io.mantisrx.common.codec.Encoder;
import io.mantisrx.common.network.WritableEndpoint;
import io.reactivex.mantis.remote.observable.ingress.IngressPolicy;
import io.reactivex.mantis.remote.observable.slotting.SlottingStrategy;
import io.reactivx.mantis.operators.DisableBackPressureOperator;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import mantis.io.reactivex.netty.channel.ConnectionHandler;
import mantis.io.reactivex.netty.channel.ObservableConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Notification;
import rx.Notification.Kind;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.GroupedObservable;


public class RemoteObservableConnectionHandler implements
        ConnectionHandler<RemoteRxEvent, List<RemoteRxEvent>> {

    private static final Logger logger = LoggerFactory.getLogger(RemoteObservableConnectionHandler.class);

    @SuppressWarnings("rawtypes")
    private Map<String, ServeConfig> observables;
    private RxMetrics serverMetrics;
    private IngressPolicy ingressPolicy;
    private int writeBufferTimeMSec;

    @SuppressWarnings("rawtypes")
    public RemoteObservableConnectionHandler(
            Map<String, ServeConfig> observables,
            IngressPolicy ingressPolicy,
            RxMetrics metrics,
            int writeBufferTimeMSec) {
        this.observables = observables;
        this.ingressPolicy = ingressPolicy;
        this.serverMetrics = metrics;
        this.writeBufferTimeMSec = writeBufferTimeMSec;
    }

    @Override
    public Observable<Void> handle(final
                                   ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection) {
        logger.info("Connection received: " + connection.getChannel().remoteAddress());
        if (ingressPolicy.allowed(connection)) {
            return setupConnection(connection);
        } else {
            Exception e = new RemoteObservableException("Connection rejected due to ingress policy");
            return Observable.error(e);
        }
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    private <T> Subscription serveObservable(
            final Observable<T> observable,
            final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection,
            final RemoteRxEvent event,
            final Func1<Map<String, String>, Func1<T, Boolean>> filterFunction,
            final Encoder<T> encoder,
            final ServeObservable<T> serveConfig,
            final WritableEndpoint<Observable<T>> endpoint) {

        final MutableReference<Subscription> subReference = new MutableReference<>();
        subReference.setValue(
                observable
                        .filter(filterFunction.call(event.getSubscribeParameters()))
                        .doOnCompleted(new Action0() {
                            @Override
                            public void call() {
                                logger.info("OnCompleted recieved in serveObservable, sending to client.");
                            }
                        })
                        .doOnError(new Action1<Throwable>() {
                            @Override
                            public void call(Throwable t1) {
                                logger.info("OnError received in serveObservable, sending to client: ", t1);
                            }
                        })
                        .materialize()
                        .lift(new DisableBackPressureOperator<Notification<T>>())
                        .buffer(writeBufferTimeMSec, TimeUnit.MILLISECONDS)
                        .filter(new Func1<List<Notification<T>>, Boolean>() {
                            @Override
                            public Boolean call(List<Notification<T>> t1) {
                                return t1 != null && !t1.isEmpty();
                            }
                        })
                        .map(new Func1<List<Notification<T>>, List<RemoteRxEvent>>() {

                            @Override
                            public List<RemoteRxEvent> call(List<Notification<T>> notifications) {

                                List<RemoteRxEvent> rxEvents = new ArrayList<RemoteRxEvent>(notifications.size());
                                for (Notification<T> notification : notifications) {


                                    if (notification.getKind() == Notification.Kind.OnNext) {
                                        rxEvents.add(RemoteRxEvent.next(event.getName(), encoder.encode(notification.getValue())));
                                    } else if (notification.getKind() == Notification.Kind.OnError) {
                                        rxEvents.add(RemoteRxEvent.error(event.getName(), RemoteObservable.fromThrowableToBytes(notification.getThrowable())));
                                    } else if (notification.getKind() == Notification.Kind.OnCompleted) {
                                        rxEvents.add(RemoteRxEvent.completed(event.getName()));
                                    } else {
                                        throw new RuntimeException("Unsupported notification kind: " + notification.getKind());
                                    }
                                }
                                return rxEvents;
                            }
                        })
                        .filter(new Func1<List<RemoteRxEvent>, Boolean>() {
                            @Override
                            public Boolean call(List<RemoteRxEvent> t1) {
                                return t1 != null && !t1.isEmpty();
                            }
                        })
                        .subscribe(new WriteBytesObserver(connection, subReference, serverMetrics,
                                serveConfig.getSlottingStrategy(), endpoint)));
        return subReference.getValue();
    }

    @SuppressWarnings( {"unchecked", "rawtypes"})
    private <T> Subscription serveNestedObservable(
            final Observable<T> observable,
            final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection,
            final RemoteRxEvent event,
            final Func1<Map<String, String>, Func1<T, Boolean>> filterFunction,
            final Encoder<T> encoder,
            final ServeNestedObservable<Observable<T>> serveConfig,
            final WritableEndpoint<Observable<T>> endpoint) {

        final MutableReference<Subscription> subReference = new MutableReference<>();
        subReference.setValue(
                observable
                        .filter(filterFunction.call(event.getSubscribeParameters()))
                        .doOnCompleted(new Action0() {
                            @Override
                            public void call() {
                                logger.info("OnCompleted recieved in serveNestedObservable, sending to client.");
                            }
                        })
                        .doOnError(new Action1<Throwable>() {
                            @Override
                            public void call(Throwable t1) {
                                logger.info("OnError received in serveNestedObservable, sending to client: ", t1);
                            }
                        })
                        .map(new Func1<T, byte[]>() {
                            @Override
                            public byte[] call(T t1) {
                                return encoder.encode(t1);
                            }
                        })
                        .materialize()
                        .map(new Func1<Notification<byte[]>, RemoteRxEvent>() {
                            @Override
                            public RemoteRxEvent call(Notification<byte[]> notification) {
                                if (notification.getKind() == Notification.Kind.OnNext) {
                                    return RemoteRxEvent.next(event.getName(), notification.getValue());
                                } else if (notification.getKind() == Notification.Kind.OnError) {
                                    return RemoteRxEvent.error(event.getName(), RemoteObservable.fromThrowableToBytes(notification.getThrowable()));
                                } else if (notification.getKind() == Notification.Kind.OnCompleted) {
                                    return RemoteRxEvent.completed(event.getName());
                                } else {
                                    throw new RuntimeException("Unsupported notification kind: " + notification.getKind());
                                }
                            }
                        })
                        .lift(new DisableBackPressureOperator<RemoteRxEvent>())
                        .buffer(writeBufferTimeMSec, TimeUnit.MILLISECONDS)
                        .filter(new Func1<List<RemoteRxEvent>, Boolean>() {
                            @Override
                            public Boolean call(List<RemoteRxEvent> t1) {
                                return t1 != null && !t1.isEmpty();
                            }
                        })
                        .filter(new Func1<List<RemoteRxEvent>, Boolean>() {
                            @Override
                            public Boolean call(List<RemoteRxEvent> t1) {
                                return t1 != null && !t1.isEmpty();
                            }
                        })
                        .subscribe(new WriteBytesObserver(connection, subReference, serverMetrics,
                                serveConfig.getSlottingStrategy(), endpoint)));
        return subReference.getValue();
    }

    private <K, V> Subscription serveGroupedObservable(
            final Observable<Group<K, V>> groups,
            final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection,
            final RemoteRxEvent event,
            final Func1<Map<String, String>, Func1<K, Boolean>> filterFunction,
            final Encoder<K> keyEncoder,
            final Encoder<V> valueEncoder,
            final ServeGroupedObservable<K, V> serveConfig,
            final WritableEndpoint<GroupedObservable<K, V>> endpoint) {

        final MutableReference<Subscription> subReference = new MutableReference<>();
        subReference.setValue(groups
                // filter out groups based on subscription parameters
                .filter(new Func1<Group<K, V>, Boolean>() {
                    @Override
                    public Boolean call(Group<K, V> group) {
                        return filterFunction.call(event.getSubscribeParameters())
                                .call(group.getKeyValue());
                    }
                })
                .doOnCompleted(new Action0() {
                    @Override
                    public void call() {
                        logger.info("OnCompleted recieved in serveGroupedObservable, sending to client.");
                    }
                })
                .doOnError(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable t1) {
                        logger.info("OnError received in serveGroupedObservable, sending to client: ", t1);
                    }
                })
                .materialize()
                .lift(new DisableBackPressureOperator<Notification<Group<K, V>>>())
                .buffer(writeBufferTimeMSec, TimeUnit.MILLISECONDS)
                .filter(new Func1<List<Notification<Group<K, V>>>, Boolean>() {
                    @Override
                    public Boolean call(List<Notification<Group<K, V>>> t1) {
                        return t1 != null && !t1.isEmpty();
                    }
                })
                .map(new Func1<List<Notification<Group<K, V>>>, List<RemoteRxEvent>>() {
                    @Override
                    public List<RemoteRxEvent> call(final List<Notification<Group<K, V>>> groupNotifications) {
                        List<RemoteRxEvent> rxEvents = new ArrayList<RemoteRxEvent>(groupNotifications.size());
                        for (Notification<Group<K, V>> groupNotification : groupNotifications) {
                            if (Kind.OnNext == groupNotification.getKind()) {
                                // encode inner group notification
                                Group<K, V> group = groupNotification.getValue();
                                final int keyLength = group.getKeyBytes().length;
                                Notification<V> notification = groupNotification.getValue().getNotification();
                                byte[] data = null;
                                if (Kind.OnNext == notification.getKind()) {
                                    V value = notification.getValue();
                                    byte[] valueBytes = valueEncoder.encode(value);
                                    // 1 byte for notification type,
                                    // 4 bytes is to encode key length as int
                                    data = ByteBuffer.allocate(1 + 4 + keyLength + valueBytes.length)
                                            .put((byte) 1)
                                            .putInt(keyLength)
                                            .put(group.getKeyBytes())
                                            .put(valueBytes)
                                            .array();
                                } else if (Kind.OnCompleted == notification.getKind()) {
                                    data = ByteBuffer.allocate(1 + 4 + keyLength)
                                            .put((byte) 2)
                                            .putInt(keyLength)
                                            .put(group.getKeyBytes())
                                            .array();
                                } else if (Kind.OnError == notification.getKind()) {
                                    Throwable error = notification.getThrowable();
                                    byte[] errorBytes = RemoteObservable.fromThrowableToBytes(error);
                                    data = ByteBuffer.allocate(1 + 4 + keyLength + errorBytes.length)
                                            .put((byte) 3)
                                            .putInt(keyLength)
                                            .put(group.getKeyBytes())
                                            .put(errorBytes)
                                            .array();
                                }
                                rxEvents.add(RemoteRxEvent.next(event.getName(), data));
                            } else if (Kind.OnCompleted == groupNotification.getKind()) {
                                rxEvents.add(RemoteRxEvent.completed(event.getName()));
                            } else if (Kind.OnError == groupNotification.getKind()) {
                                rxEvents.add(RemoteRxEvent.error(event.getName(),
                                        RemoteObservable.fromThrowableToBytes(groupNotification.getThrowable())));
                            } else {
                                throw new RuntimeException("Unsupported notification type: " + groupNotification.getKind());
                            }
                        }
                        return rxEvents;
                    }
                })
                .filter(new Func1<List<RemoteRxEvent>, Boolean>() {
                    @Override
                    public Boolean call(List<RemoteRxEvent> t1) {
                        return t1 != null && !t1.isEmpty();
                    }
                })
                .subscribe(new WriteBytesObserver(connection, subReference, serverMetrics,
                        serveConfig.getSlottingStrategy(), endpoint)));
        return subReference.getValue();
    }

    @SuppressWarnings( {"unchecked", "rawtypes"})
    private void subscribe(final MutableReference<Subscription> unsubscribeCallbackReference,
                           RemoteRxEvent event, final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection,
                           ServeConfig configuration, WritableEndpoint endpoint) {
        Func1 filterFunction = configuration.getFilterFunction();

        Subscription subscription = null;
        if (configuration instanceof ServeObservable) {
            ServeObservable serveObservable = (ServeObservable) configuration;
            if (serveObservable.isSubscriptionPerConnection()) {
                Observable toServe = serveObservable.getObservable();
                if (serveObservable.isHotStream()) {
                    toServe = toServe.share();
                }
                subscription = serveObservable(toServe, connection,
                        event, filterFunction, serveObservable.getEncoder(), serveObservable, endpoint);
            } else {
                subscription = serveObservable(endpoint.read(), connection,
                        event, filterFunction, serveObservable.getEncoder(), serveObservable, endpoint);
            }
        } else if (configuration instanceof ServeGroupedObservable) {
            ServeGroupedObservable sgo = (ServeGroupedObservable) configuration;
            subscription = serveGroupedObservable(endpoint.read(), connection,
                    event, filterFunction, sgo.getKeyEncoder(), sgo.getValueEncoder(),
                    sgo, endpoint);
        } else if (configuration instanceof ServeNestedObservable) {
            ServeNestedObservable serveNestedObservable = (ServeNestedObservable) configuration;
            subscription = serveNestedObservable(endpoint.read(), connection,
                    event, filterFunction, serveNestedObservable.getEncoder(), serveNestedObservable, endpoint);
        }

        unsubscribeCallbackReference.setValue(subscription);
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    private Observable<Void> handleSubscribeRequest(RemoteRxEvent event, final ObservableConnection<RemoteRxEvent,
            List<RemoteRxEvent>> connection, MutableReference<SlottingStrategy> slottingStrategyReference,
                                                    MutableReference<Subscription> unsubscribeCallbackReference,
                                                    MutableReference<WritableEndpoint> slottingIdReference) {

        // check if observable exists in configs
        String observableName = event.getName();
        ServeConfig config = observables.get(observableName);
        if (config == null) {
            return Observable.error(new RemoteObservableException("No remote observable configuration found for name: " + observableName));
        }

        if (event.getType() == RemoteRxEvent.Type.subscribed) {
            String slotId = null;
            Map<String, String> subscriptionParameters = event.getSubscribeParameters();
            if (subscriptionParameters != null) {
                slotId = subscriptionParameters.get("slotId");
            }

            InetSocketAddress address = (InetSocketAddress) connection.getChannel().remoteAddress();
            WritableEndpoint endpoint = new WritableEndpoint<>(address.getHostName(), address.getPort(), slotId,
                    connection);
            SlottingStrategy slottingStrategy = config.getSlottingStrategy();

            slottingIdReference.setValue(endpoint);
            slottingStrategyReference.setValue(slottingStrategy);

            logger.info("Connection received on server from client endpoint: " + endpoint + ", subscribed to observable: " + observableName);
            serverMetrics.incrementSubscribedCount();
            subscribe(unsubscribeCallbackReference, event, connection, config, endpoint);
            if (!slottingStrategy.addConnection(endpoint)) {
                // unable to slot connection
                logger.warn("Failed to slot connection for endpoint: " + endpoint);
                connection.close(true);
            }
        }
        return Observable.empty();
    }

    @SuppressWarnings("rawtypes")
    private Observable<Void> setupConnection(final ObservableConnection<RemoteRxEvent, List<RemoteRxEvent>> connection) {

        // state associated with connection
        // used to initiate 'unsubscribe' callback to subscriber
        final MutableReference<Subscription> unsubscribeCallbackReference = new MutableReference<Subscription>();
        // used to release slot when connection completes
        final MutableReference<SlottingStrategy> slottingStrategyReference = new MutableReference<SlottingStrategy>();
        // used to get slotId for connection
        final MutableReference<WritableEndpoint> slottingIdReference = new MutableReference<WritableEndpoint>();

        return connection.getInput()
                // filter out unsupported operations
                .filter(new Func1<RemoteRxEvent, Boolean>() {
                    @Override
                    public Boolean call(RemoteRxEvent event) {
                        boolean supportedOperation = false;
                        if (event.getType() == RemoteRxEvent.Type.subscribed ||
                                event.getType() == RemoteRxEvent.Type.unsubscribed) {
                            supportedOperation = true;
                        }
                        return supportedOperation;
                    }
                })
                .flatMap(new Func1<RemoteRxEvent, Observable<Void>>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public Observable<Void> call(RemoteRxEvent event) {
                        if (event.getType() == RemoteRxEvent.Type.subscribed) {
                            return handleSubscribeRequest(event, connection, slottingStrategyReference, unsubscribeCallbackReference,
                                    slottingIdReference);
                        } else if (event.getType() == RemoteRxEvent.Type.unsubscribed) {
                            Subscription subscription = unsubscribeCallbackReference.getValue();
                            if (subscription != null) {
                                subscription.unsubscribe();
                            }
                            serverMetrics.incrementUnsubscribedCount();
                            // release slot
                            if (slottingStrategyReference.getValue() != null) {
                                if (!slottingStrategyReference.getValue().removeConnection(slottingIdReference.getValue())) {
                                    logger.error("Failed to remove endpoint from slot,  endpoint: " + slottingIdReference.getValue());
                                }
                            }
                            logger.info("Connection: " + connection.getChannel()
                                    .remoteAddress() + " unsubscribed, closing connection");
                            connection.close(true);
                        }
                        return Observable.empty();
                    }
                });
    }

}