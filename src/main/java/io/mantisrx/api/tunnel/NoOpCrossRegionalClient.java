package io.mantisrx.api.tunnel;

import io.netty.buffer.ByteBuf;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;

public class NoOpCrossRegionalClient implements MantisCrossRegionalClient {
    @Override
    public HttpClient<ByteBuf, ServerSentEvent> getSecureSseClient(String region) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpClient<String, ByteBuf> getSecureRestClient(String region) {
        throw new UnsupportedOperationException();
    }
}
