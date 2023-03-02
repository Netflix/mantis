package io.mantisrx.api.push;

import com.netflix.config.DynamicIntProperty;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.MantisMasterGateway;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import rx.Observable;
import rx.Subscription;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Http handler for the WebSocket/SSE paths.
 */
@Slf4j
public class MantisSSEHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final DynamicIntProperty queueCapacity = new DynamicIntProperty("io.mantisrx.api.push.queueCapacity", 1000);
    private final DynamicIntProperty writeIntervalMillis = new DynamicIntProperty("io.mantisrx.api.push.writeIntervalMillis", 50);

    private final ConnectionBroker connectionBroker;
    private final HighAvailabilityServices highAvailabilityServices;
    private final List<String> pushPrefixes;

    private Subscription subscription;
    private String uri;

    public MantisSSEHandler(ConnectionBroker connectionBroker, HighAvailabilityServices highAvailabilityServices,
                            List<String> pushPrefixes) {
        super(true);
        this.connectionBroker = connectionBroker;
        this.highAvailabilityServices = highAvailabilityServices;
        this.pushPrefixes = pushPrefixes;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (Util.startsWithAnyOf(request.uri(), pushPrefixes)
                && !isWebsocketUpgrade(request)) {

            if (HttpUtil.is100ContinueExpected(request)) {
                send100Contine(ctx);
            }

            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK);
            HttpHeaders headers = response.headers();
            headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
            headers.set(HttpHeaderNames.CONTENT_TYPE, "text/event-stream");
            headers.set(HttpHeaderNames.CACHE_CONTROL, "no-cache, no-store, max-age=0, must-revalidate");
            headers.set(HttpHeaderNames.PRAGMA, HttpHeaderValues.NO_CACHE);
            headers.set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);

            uri = request.uri();
            final PushConnectionDetails pcd =
                    isSubmitAndConnect(request)
                            ? new PushConnectionDetails(uri, jobSubmit(request), PushConnectionDetails.TARGET_TYPE.CONNECT_BY_ID, io.vavr.collection.List.empty())
                            : PushConnectionDetails.from(uri);
            log.info("SSE Connecting for: {}", pcd);

            boolean tunnelPingsEnabled = isTunnelPingsEnabled(uri);

            final String[] tags = Util.getTaglist(uri, pcd.target);
            Counter numDroppedBytesCounter = SpectatorUtils.newCounter(Constants.numDroppedBytesCounterName, pcd.target, tags);
            Counter numDroppedMessagesCounter = SpectatorUtils.newCounter(Constants.numDroppedMessagesCounterName, pcd.target, tags);
            Counter numMessagesCounter = SpectatorUtils.newCounter(Constants.numMessagesCounterName, pcd.target, tags);
            Counter numBytesCounter = SpectatorUtils.newCounter(Constants.numBytesCounterName, pcd.target, tags);

            BlockingQueue<String> queue = new LinkedBlockingQueue<String>(queueCapacity.get());

            this.subscription = this.connectionBroker.connect(pcd)
                    .mergeWith(tunnelPingsEnabled
                            ? Observable.interval(Constants.TunnelPingIntervalSecs, Constants.TunnelPingIntervalSecs,
                            TimeUnit.SECONDS)
                            .map(l -> Constants.TunnelPingMessage)
                            : Observable.empty())
                    .mergeWith(Observable.interval(writeIntervalMillis.get(), TimeUnit.MILLISECONDS)
                                .map(__ -> Constants.DUMMY_TIMER_DATA))
                    .doOnNext(event -> {
                        if (!Constants.DUMMY_TIMER_DATA.equals(event)) {
                          String data = Constants.SSE_DATA_PREFIX + event + Constants.SSE_DATA_SUFFIX;
                          if (!queue.offer(data)) {
                              numDroppedBytesCounter.increment(data.length());
                              numDroppedMessagesCounter.increment();
                          }
                        }
                    })
                    .filter(Constants.DUMMY_TIMER_DATA::equals)
                    .doOnNext(__ -> {
                        if (ctx.channel().isWritable()) {
                            final List<String> items = new ArrayList<>(queue.size());
                            queue.drainTo(items);
                            for (String data : items) {
                              ctx.writeAndFlush(Unpooled.copiedBuffer(data, StandardCharsets.UTF_8));
                              numMessagesCounter.increment();
                              numBytesCounter.increment(data.length());
                            }
                        }
                    })
                    .subscribe();
        } else {
            ctx.fireChannelRead(request.retain());
        }
    }

    private static void send100Contine(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.CONTINUE);
        ctx.writeAndFlush(response);
    }

    private boolean isTunnelPingsEnabled(String uri) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
        return queryStringDecoder.parameters()
                .getOrDefault(Constants.TunnelPingParamName, Arrays.asList("false"))
                .get(0)
                .equalsIgnoreCase("true");
    }

    private boolean isWebsocketUpgrade(HttpRequest request) {
        HttpHeaders headers = request.headers();
        // Header "Connection" contains "upgrade" (case insensitive) and
        // Header "Upgrade" equals "websocket" (case insensitive)
        String connection = headers.get(HttpHeaderNames.CONNECTION);
        String upgrade = headers.get(HttpHeaderNames.UPGRADE);
        return connection != null && connection.toLowerCase().contains("upgrade") &&
                upgrade != null && upgrade.toLowerCase().equals("websocket");
    }


    private boolean isSubmitAndConnect(HttpRequest request) {
        return request.method().equals(HttpMethod.POST) && request.uri().contains("jobsubmitandconnect");
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel {} is unregistered. URI: {}", ctx.channel(), uri);
        unsubscribeIfSubscribed();
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel {} is inactive. URI: {}", ctx.channel(), uri);
        unsubscribeIfSubscribed();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception caught by channel {}. URI: {}", ctx.channel(), uri, cause);
        unsubscribeIfSubscribed();
        ctx.close();
    }

    /** Unsubscribe if it's subscribed. */
    private void unsubscribeIfSubscribed() {
        if (subscription != null && !subscription.isUnsubscribed()) {
            log.info("SSE unsubscribing subscription with URI: {}", uri);
            subscription.unsubscribe();
        }
    }

    public String jobSubmit(FullHttpRequest request) {
        final String API_JOB_SUBMIT_PATH = "/api/submit";

        String content = request.content().toString(StandardCharsets.UTF_8);
        return callPostOnMaster(highAvailabilityServices.getMasterMonitor().getMasterObservable(), API_JOB_SUBMIT_PATH, content)
                .retryWhen(Util.getRetryFunc(log, API_JOB_SUBMIT_PATH))
                .flatMap(masterResponse -> masterResponse.getByteBuf()
                        .take(1)
                        .map(byteBuf -> {
                            final String s = byteBuf.toString(StandardCharsets.UTF_8);
                            log.info("response: " + s);
                            return s;
                        }))
                .take(1)
                .toBlocking()
                .first();
    }

    public static class MasterResponse {

        private final HttpResponseStatus status;
        private final Observable<ByteBuf> byteBuf;
        private final HttpResponseHeaders responseHeaders;

        public MasterResponse(HttpResponseStatus status, Observable<ByteBuf> byteBuf, HttpResponseHeaders responseHeaders) {
            this.status = status;
            this.byteBuf = byteBuf;
            this.responseHeaders = responseHeaders;
        }

        public HttpResponseStatus getStatus() {
            return status;
        }

        public Observable<ByteBuf> getByteBuf() {
            return byteBuf;
        }

        public HttpResponseHeaders getResponseHeaders() { return responseHeaders; }
    }

    public static Observable<MasterResponse> callPostOnMaster(Observable<MasterDescription> masterObservable, String uri, String content) {
        PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<String>> pipelineConfigurator
                = PipelineConfigurators.httpClientConfigurator();

        return masterObservable
                .filter(Objects::nonNull)
                .flatMap(masterDesc -> {
                    HttpClient<String, ByteBuf> client =
                            RxNetty.<String, ByteBuf>newHttpClientBuilder(masterDesc.getHostname(), masterDesc.getApiPort())
                                    .pipelineConfigurator(pipelineConfigurator)
                                    .build();
                    HttpClientRequest<String> request = HttpClientRequest.create(HttpMethod.POST, uri);
                    request = request.withHeader(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
                    request.withRawContent(content, StringTransformer.DEFAULT_INSTANCE);
                    return client.submit(request)
                            .map(response -> new MasterResponse(response.getStatus(), response.getContent(), response.getHeaders()));
                })
                .take(1);
    }
}
