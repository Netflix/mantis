package io.mantisrx.api.push;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

import com.netflix.config.DynamicIntProperty;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.Subscription;

@Slf4j
public class MantisWebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    private final ConnectionBroker connectionBroker;
    private final DynamicIntProperty queueCapacity = new DynamicIntProperty("io.mantisrx.api.push.queueCapacity", 1000);
    private final DynamicIntProperty writeIntervalMillis = new DynamicIntProperty("io.mantisrx.api.push.writeIntervalMillis", 50);

    private Subscription subscription;
    private String uri;

    public MantisWebSocketFrameHandler(ConnectionBroker broker) {
        super(true);
        this.connectionBroker = broker;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt.getClass() == WebSocketServerProtocolHandler.HandshakeComplete.class) {
            WebSocketServerProtocolHandler.HandshakeComplete complete = (WebSocketServerProtocolHandler.HandshakeComplete) evt;

            uri = complete.requestUri();
            final PushConnectionDetails pcd = PushConnectionDetails.from(uri);

            log.info("Request to URI '{}' is a WebSSocket upgrade, removing the SSE handler", uri);
            if (ctx.pipeline().get(MantisSSEHandler.class) != null) {
                ctx.pipeline().remove(MantisSSEHandler.class);
            }

            final String[] tags = Util.getTaglist(uri, pcd.target);
            Counter numDroppedBytesCounter = SpectatorUtils.newCounter(Constants.numDroppedBytesCounterName, pcd.target, tags);
            Counter numDroppedMessagesCounter = SpectatorUtils.newCounter(Constants.numDroppedMessagesCounterName, pcd.target, tags);
            Counter numMessagesCounter = SpectatorUtils.newCounter(Constants.numMessagesCounterName, pcd.target, tags);
            Counter numBytesCounter = SpectatorUtils.newCounter(Constants.numBytesCounterName, pcd.target, tags);

            BlockingQueue<String> queue = new LinkedBlockingQueue<>(queueCapacity.get());

            this.subscription = this.connectionBroker.connect(pcd)
                    .mergeWith(Observable.interval(writeIntervalMillis.get(), TimeUnit.MILLISECONDS)
                                 .map(__ -> Constants.DUMMY_TIMER_DATA))
                    .doOnNext(event -> {
                        if (!Constants.DUMMY_TIMER_DATA.equals(event) && !queue.offer(event)) {
                            numDroppedBytesCounter.increment(event.length());
                            numDroppedMessagesCounter.increment();
                        }
                    })
                    .filter(Constants.DUMMY_TIMER_DATA::equals)
                    .doOnNext(__ -> {
                        if (ctx.channel().isWritable()) {
                            final List<String> items = new ArrayList<>(queue.size());
                            queue.drainTo(items);
                            for (String event : items) {
                                ctx.writeAndFlush(new TextWebSocketFrame(event));
                                numMessagesCounter.increment();
                                numBytesCounter.increment(event.length());
                            }
                        }
                    })
                    .subscribe();
        } else {
            ReferenceCountUtil.retain(evt);
            super.userEventTriggered(ctx, evt);
        }
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
        // This is the tail of handlers. We should close the channel between the server and the client,
        // essentially causing the client to disconnect and terminate.
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) {
        // No op.
    }

    /** Unsubscribe if it's subscribed. */
    private void unsubscribeIfSubscribed() {
        if (subscription != null && !subscription.isUnsubscribed()) {
            log.info("WebSocket unsubscribing subscription with URI: {}", uri);
            subscription.unsubscribe();
        }
    }
}
