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

package io.mantisrx.runtime.sink;

import com.mantisrx.common.utils.MantisSSEConstants;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;
import io.mantisrx.common.compression.CompressionUtils;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.network.Endpoint;
import io.mantisrx.common.network.HashFunctions;
import io.mantisrx.common.network.ServerSlotManager;
import io.mantisrx.common.network.ServerSlotManager.SlotAssignmentManager;
import io.mantisrx.common.network.WritableEndpoint;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.sink.predicate.Predicate;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivx.mantis.operators.DisableBackPressureOperator;
import io.reactivx.mantis.operators.DropOperator;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerRequest;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import mantis.io.reactivex.netty.protocol.http.server.RequestHandler;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;


public class ServerSentEventRequestHandler<T> implements
        RequestHandler<ByteBuf, ServerSentEvent> {

    protected static final Object BINARY_FORMAT = "binary";
    private static final String TWO_NEWLINES = "\n\n";
    private static final String SSE_DATA_PREFIX = "data: ";
    private static final Logger LOG = LoggerFactory.getLogger(ServerSentEventRequestHandler.class);
    private static final String ENABLE_PINGS_PARAM = "enablePings";
    private static final String SAMPLE_PARAM = "sample";
    private static final String SAMPLE_PARAM_MSEC = "sampleMSec";
    private static final String CLIENT_ID_PARAM = "clientId";
    private static final int PING_INTERVAL = 2000;
    private static final String TEXT_FORMAT = "text";
    private static final String DEFAULT_FORMAT = TEXT_FORMAT;

    private static final String FORMAT_PARAM = "format";
    private static final byte[] EVENT_PREFIX_BYTES = "event: ".getBytes();
    private static final byte[] NEW_LINE_AS_BYTES = TWO_NEWLINES.getBytes();
    private static final byte[] ID_PREFIX_AS_BYTES = "id: ".getBytes();
    private static final byte[] DATA_PREFIX_AS_BYTES = SSE_DATA_PREFIX.getBytes();
    private static final String PING = "\ndata: ping\n\n";
    final ServerSlotManager<String> ssm = new ServerSlotManager<>(HashFunctions.ketama());
    private final Observable<T> observableToServe;
    private final Func1<T, String> encoder;
    private final Func1<Throwable, String> errorEncoder;
    private final Predicate<T> predicate;
    private final Func2<Map<String, List<String>>, Context, Void> requestPreprocessor;
    private final Func2<Map<String, List<String>>, Context, Void> requestPostprocessor;
    private final Context context;
    private boolean pingsEnabled = true;
    private int flushIntervalMillis = 250;
    private String format = DEFAULT_FORMAT;

    public ServerSentEventRequestHandler(Observable<T> observableToServe,
                                  Func1<T, String> encoder,
                                  Func1<Throwable, String> errorEncoder,
                                  Predicate<T> predicate,
                                  Func2<Map<String, List<String>>, Context, Void> requestPreprocessor,
                                  Func2<Map<String, List<String>>, Context, Void> requestPostprocessor,
                                  Context context,
                                  int batchInterval) {
        this.observableToServe = observableToServe;
        this.encoder = encoder;
        this.errorEncoder = errorEncoder;
        this.predicate = predicate;
        this.requestPreprocessor = requestPreprocessor;
        this.requestPostprocessor = requestPostprocessor;
        this.context = context;
        this.flushIntervalMillis = batchInterval;
    }

    @Override
    public Observable<Void> handle(HttpServerRequest<ByteBuf> request,
                                   final HttpServerResponse<ServerSentEvent> response) {
        InetSocketAddress socketAddress = (InetSocketAddress) response.getChannel().remoteAddress();
        LOG.info("HTTP SSE connection received from " + socketAddress.getAddress() + ":" + socketAddress.getPort() + "  queryParams: " + request.getQueryParameters());
        final String socketAddrStr = socketAddress.getAddress().toString();

        final WritableEndpoint<String> sn = new WritableEndpoint<>(socketAddress.getHostString(),
                socketAddress.getPort(), Endpoint.uniqueHost(socketAddress.getHostString(), socketAddress.getPort(), null));

        final Map<String, List<String>> queryParameters = request.getQueryParameters();

        final SlotAssignmentManager<String> slotMgr = ssm.registerServer(sn, queryParameters);

        final AtomicLong lastResponseFlush = new AtomicLong();
        lastResponseFlush.set(-1);

        final AtomicLong lastResponseSent = new AtomicLong(-1);
        // copy reference, then apply request specific filters, sampling
        Observable<T> requestObservable = observableToServe;

        // decouple the observable on a separate thread and add backpressure handling
        String decoupleSSE = "false";//ServiceRegistry.INSTANCE.getPropertiesService().getStringValue("sse.decouple", "false");
        //TODO Below condition would be always false during if condition.
        // Since decoupleSSE would be false and matching with true as string
        // would always ignore code inside if block
        if ("true".equals(decoupleSSE)) {
            final BasicTag sockAddrTag = new BasicTag("sockAddr", Optional.ofNullable(socketAddrStr).orElse("none"));
            requestObservable = requestObservable
                    .lift(new DropOperator<>("outgoing_ServerSentEventRequestHandler", sockAddrTag))
                    .observeOn(Schedulers.io());
        }
        response.getHeaders().set("Access-Control-Allow-Origin", "*");
        response.getHeaders().set("content-type", "text/event-stream");
        response.getHeaders().set("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
        response.getHeaders().set("Pragma", "no-cache");
        response.flush();

        String uniqueClientId = socketAddrStr;

        Tag[] tags = new Tag[2];
        final String clientId = Optional.ofNullable(uniqueClientId).orElse("none");
        final String sockAddr = Optional.ofNullable(socketAddrStr).orElse("none");
        tags[0] = new BasicTag("clientId", clientId);
        tags[1] = new BasicTag("sockAddr", sockAddr);

        Metrics sseSinkMetrics = new Metrics.Builder()
                .id("ServerSentEventRequestHandler", tags)
                .addCounter("processedCounter")
                .addCounter("pingCounter")
                .addCounter("errorCounter")
                .addCounter("droppedCounter")
                .addCounter("flushCounter")
                .addCounter("sourceJobNameMismatchRejection")
                .build();


        final Counter msgProcessedCounter = sseSinkMetrics.getCounter("processedCounter");
        final Counter pingCounter = sseSinkMetrics.getCounter("pingCounter");
        final Counter errorCounter = sseSinkMetrics.getCounter("errorCounter");
        final Counter droppedWrites = sseSinkMetrics.getCounter("droppedCounter");
        final Counter flushCounter = sseSinkMetrics.getCounter("flushCounter");
        final Counter sourceJobNameMismatchRejectionCounter = sseSinkMetrics.getCounter("sourceJobNameMismatchRejection");


        if (queryParameters != null && queryParameters.containsKey(MantisSSEConstants.TARGET_JOB)) {
            String targetJob = queryParameters.get(MantisSSEConstants.TARGET_JOB).get(0);
            String currentJob = this.context.getWorkerInfo().getJobClusterName();
            if (!currentJob.equalsIgnoreCase(targetJob)) {
                LOG.info("Rejecting connection from {}. Client is targeting job {} but this is job {}.", uniqueClientId, targetJob, currentJob);
                sourceJobNameMismatchRejectionCounter.increment();
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
                response.writeStringAndFlush("data: " + MantisSSEConstants.TARGET_JOB + " is " + targetJob + " but this is " + currentJob + "." + TWO_NEWLINES);
                return response.close();
            }
        }

        if (queryParameters != null && queryParameters.containsKey(CLIENT_ID_PARAM)) {
            // enablePings
            uniqueClientId = queryParameters.get(CLIENT_ID_PARAM).get(0);

        }

        if (queryParameters != null && queryParameters.containsKey(FORMAT_PARAM)) {

            format = queryParameters.get(FORMAT_PARAM).get(0);

        }

        if (queryParameters != null && requestPreprocessor != null) {

            requestPreprocessor.call(queryParameters, context);
        }

        // apply sampling, milli, then seconds
        if (queryParameters != null && queryParameters.containsKey(SAMPLE_PARAM_MSEC)) {
            // apply sampling rate
            int samplingRate = Integer.parseInt(queryParameters.get(SAMPLE_PARAM_MSEC).get(0));
            requestObservable = requestObservable.sample(samplingRate, TimeUnit.MILLISECONDS);
        }

        if (queryParameters != null && queryParameters.containsKey(SAMPLE_PARAM)) {
            // apply sampling rate
            int samplingRate = Integer.parseInt(queryParameters.get(SAMPLE_PARAM).get(0));
            requestObservable = requestObservable.sample(samplingRate, TimeUnit.SECONDS);
        }

        if (queryParameters != null && queryParameters.containsKey(ENABLE_PINGS_PARAM)) {
            // enablePings
            String enablePings = queryParameters.get(ENABLE_PINGS_PARAM).get(0);
            //TODO Note: Code logic can be improved here.
            // since if condition check returns same true or false which can equated to pingsEnabled value.
            if ("true".equalsIgnoreCase(enablePings)) {
                pingsEnabled = true;
            } else {
                pingsEnabled = false;
            }
        }

        if (queryParameters != null && queryParameters.containsKey("delay")) {
            // apply flush
            try {
                int flushInterval = Integer.parseInt(queryParameters.get("delay").get(0));
                if (flushInterval >= 50) {
                    flushIntervalMillis = flushInterval;
                } else {
                    LOG.warn("delay parameter too small " + flushInterval + " min. is 100");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        final byte[] delimiter = queryParameters != null
                && queryParameters.containsKey(MantisSSEConstants.MANTIS_COMPRESSION_DELIMITER)
                && queryParameters.get(MantisSSEConstants.MANTIS_COMPRESSION_DELIMITER).get(0) != null
                ? queryParameters.get(MantisSSEConstants.MANTIS_COMPRESSION_DELIMITER).get(0).getBytes()
                : null;

        // get predicate, defaults to return true for all T
        Func1<T, Boolean> filterFunction = new Func1<T, Boolean>() {
            @Override
            public Boolean call(T t1) {
                return true;
            }
        };
        if (queryParameters != null && predicate != null) {
            filterFunction = predicate.getPredicate().call(queryParameters);
        }

        final Subscription timerSubscription = Observable.interval(1, TimeUnit.SECONDS).doOnNext(new Action1<Long>() {
            @Override
            public void call(Long t1) {
                long currentTime = System.currentTimeMillis();
                if (pingsEnabled && (lastResponseSent.get() == -1 || currentTime > lastResponseSent.get() + PING_INTERVAL)) {
                    pingCounter.increment();

                    response.writeStringAndFlush(PING);

                    lastResponseSent.set(currentTime);
                }
            }
        }).subscribe();

        return requestObservable

                .filter(filterFunction)
                .map(encoder)
                .lift(new DisableBackPressureOperator<>())
                .buffer(flushIntervalMillis, TimeUnit.MILLISECONDS)
                .flatMap(new Func1<List<String>, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(List<String> valueList) {
                        if (response.isCloseIssued() || !response.getChannel().isActive()) {
                            LOG.info("Client closed detected, throwing closed channel exception");
                            return Observable.error(new ClosedChannelException());
                        }

                        List<String> filteredList = valueList.stream().filter(e -> {
                            return slotMgr.filter(sn, e.getBytes());
                        }).collect(Collectors.toList());
                        if (response.getChannel().isWritable()) {


                            flushCounter.increment();

                            if (format.equals(BINARY_FORMAT)) {
                                boolean useSnappy = true;
                                try {
                                    String compressedList = delimiter == null
                                            ? CompressionUtils.compressAndBase64Encode(filteredList, useSnappy)
                                            : CompressionUtils.compressAndBase64Encode(filteredList, useSnappy, delimiter);
                                    StringBuilder sb = new StringBuilder(3);
                                    sb.append(SSE_DATA_PREFIX);
                                    sb.append(compressedList);
                                    sb.append(TWO_NEWLINES);

                                    msgProcessedCounter.increment(valueList.size());
                                    lastResponseSent.set(System.currentTimeMillis());
                                    return response.writeStringAndFlush(sb.toString());
                                } catch (Exception e) {
                                    LOG.warn("Could not compress data" + e.getMessage());
                                    droppedWrites.increment(valueList.size());
                                    return Observable.empty();
                                }
                            } else {
                                int noOfMsgs = 0;
                                StringBuilder sb = new StringBuilder(valueList.size() * 3);
                                for (String s : filteredList) {

                                    sb.append(SSE_DATA_PREFIX);
                                    sb.append(s);
                                    sb.append(TWO_NEWLINES);
                                    noOfMsgs++;


                                }
                                msgProcessedCounter.increment(noOfMsgs);
                                lastResponseSent.set(System.currentTimeMillis());
                                return response.writeStringAndFlush(sb.toString());
                            }

                        } else {
                            //
                            droppedWrites.increment(filteredList.size());
                        }
                        return Observable.empty();
                    }
                })
                .onErrorResumeNext(new Func1<Throwable, Observable<? extends Void>>() {
                    @Override
                    public Observable<? extends Void> call(Throwable throwable) {
                        Throwable cause = throwable.getCause();
                        // ignore closed channel exceptions, this is
                        // when the connection was closed on the client
                        // side without informing the server
                        errorCounter.increment();
                        if (cause != null && !(cause instanceof ClosedChannelException)) {
                            LOG.warn("Error detected in SSE sink", cause);
                            if (errorEncoder != null) {
                                // write error out on connection
                                //response.writeAndFlush(errorEncoder.call(throwable));
                                ByteBuf errType = response.getAllocator().buffer().writeBytes("error: ".getBytes());
                                ByteBuf errRes = response.getAllocator().buffer().writeBytes((errorEncoder.call(throwable)).getBytes());
                                response.writeAndFlush(ServerSentEvent.withEventType(errType, errRes));
                            }
                            throwable.printStackTrace();
                        }
                        if (requestPostprocessor != null && queryParameters != null) {
                            requestPostprocessor.call(queryParameters, context);
                        }
                        ssm.deregisterServer(sn, queryParameters);
                        timerSubscription.unsubscribe();
                        return Observable.error(throwable);
                    }
                });
    }
}
